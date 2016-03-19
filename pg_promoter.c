/* -------------------------------------------------------------------------
 *
 * pg_promoter.c
 *
 * Simple clustering extension module for PostgreSQL.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "tcop/utility.h"
#include "libpq-int.h"

#define	HEARTBEAT_SQL "select 1;"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		PromoterMain(Datum);
static void doPromote(void);
static bool heartbeatPrimaryServer(void);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	promoter_keepalives;
static char	*promoter_primary_conninfo = NULL;
static char *trigger_file = NULL;

/* Variables for connections */
static char conninfo[MAXPGPATH];

/* Variables for cluster management */
static int retry_count;

typedef struct worktable
{
	const char *schema;
	const char *name;
} worktable;


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_promoter_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_promoter_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

/*
 * Initialize several parameters for a worker process
 */
static void
initialize()
{
	PGconn *con;

	/* Set up variables */
	snprintf(conninfo, MAXPGPATH, "%s", promoter_primary_conninfo);
	retry_count = 0;

	/* Connection confirm */
	if(!(con = PQconnectdb(conninfo)))
	{
		ereport(LOG,
				(errmsg("could not establish connection to primary server : %s", conninfo)));
		proc_exit(1);
	}

	PQfinish(con);
	return;
}

/*
 * headbeatPrimaryServer()
 *
 * This fucntion does heatbeating to primary server. If could not establish connection
 * to primary server, or primary server didn't reaction, return false.
 */
bool
heartbeatPrimaryServer(void)
{
	PGconn		*con;
	PGresult 	*res;

	/* Try to connect to primary server */
	if ((con = PQconnectdb(conninfo)) == NULL)
	{
		ereport(LOG,
				(errmsg("Could not establish conenction to primary server at %d time(s)",
						(retry_count + 1))));
		PQfinish(con);
		return false;
	}

	res = PQexec(con, HEARTBEAT_SQL);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(LOG,
				(errmsg("could not get tuple from primary server at %d time(s)",
						(retry_count + 1))));
		PQfinish(con);
		return false;
	}

	PQfinish(con);
	/* Primary server is alive now */
	return true;
}

/* Main routine */
void
PromoterMain(Datum main_arg)
{
	initialize();
		
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_promoter_sighup);
	pqsignal(SIGTERM, pg_promoter_sigterm);
	
	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int		rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   promoter_keepalives * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* If got SIGHUP, Just reload the configuration file */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* If heartbeat is failed, do promote */
		if (!heartbeatPrimaryServer())
			retry_count++;

		/* If could not connect to primary server, do promote */
		/* TODO : Must specify retry count via configuration parameter */
		if (retry_count >= 5)
		{
			doPromote();
			proc_exit(0);
		}
	}

	proc_exit(1);
}

/*
 * doPromote()
 *
 * Promote standby server using ordinally way which is used by
 * pg_ctl client tool. Put trigger file into $PGDATA, and send
 * SIGUSR1 signal to standby server.
 */
static void
doPromote(void)
{
	char trigger_filepath[MAXPGPATH];
	FILE *fp;

    snprintf(trigger_filepath, 1000, "%s/%s", DataDir, trigger_file);

	if ((fp = fopen(trigger_filepath, "w")) == NULL)
	{
		ereport(LOG,
				(errmsg("could not create promote file: \"%s\"", trigger_filepath)));
		proc_exit(1);
	}

	if (fclose(fp))
	{
		ereport(LOG,
				(errmsg("could not close promote file: \"%s\"", trigger_filepath)));
		proc_exit(1);
	}

	ereport(LOG,
			(errmsg("promote standby server to primary server")));

	/* Do promotion */
	if (kill(PostmasterPid, SIGUSR1) != 0)
	{
		ereport(LOG,
				(errmsg("failed to send SIGUSR1 signal to postmaster process : %d",
						PostmasterPid)));
		proc_exit(1);
	}
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* get the configuration */
	DefineCustomIntVariable("pg_promoter.keepalives",
							"Specific time between polling to primary server",
							NULL,
							&promoter_keepalives,
							3,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pg_promoter.primary_conninfo",
							"Connection information for primary server",
							NULL,
							&promoter_primary_conninfo,
							"",
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pg_promoter.trigger_file",
							"Trigger file for promotion to primary server",
							NULL,
							&trigger_file,
							"promote",
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = PromoterMain;
	worker.bgw_notify_pid = 0;
	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_promoter");
	worker.bgw_main_arg = Int32GetDatum(1);
	RegisterBackgroundWorker(&worker);
}
