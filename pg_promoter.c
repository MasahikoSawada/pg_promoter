/* -------------------------------------------------------------------------
 *
 * pg_promoter.c
 *
 * Created by Masahiko Sawada
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
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include "libpq-int.h"

PG_MODULE_MAGIC;

typedef long pgpid_t;

void		_PG_init(void);
void		pg_promoter_main(Datum);
static void do_promote(void);
static pgpid_t get_pgpid(void);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	pg_promoter_keepalives;
static char	*pg_promoter_primary_conninfo = NULL;

static char promote_file[MAXPGPATH];
static char pid_file[MAXPGPATH];
static char *pg_data = NULL;
static char conninfo[MAXPGPATH];

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
 * Initialize several parameter for a worker process
 */
static void
initialize_pg_promoter()
{
	PGconn *con;

	pg_data = getenv("PGDATA");

	snprintf(conninfo,MAXPGPATH,"%s",pg_promoter_primary_conninfo);

	snprintf(pid_file, 1000, "%s/postmaster.pid", pg_data);

	/* Connection confirm */	
	if(!(con = PQconnectdb(conninfo)))
	{
		elog(WARNING,"Connection confirm failed");
		elog(WARNING,"Could not establish connection to primary server : %s", conninfo);
		proc_exit(1);
	}
	return;
}

/*
 * Main
 */
void
pg_promoter_main(Datum main_arg)
{

	PGconn *con;
	PGresult *res;

	initialize_pg_promoter();
		
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
		int			rc;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pg_promoter_keepalives * 1000L);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*  Try to connect primary server */
		if (!(con = PQconnectdb(conninfo)))
		{
			/* If could not establish connection to primary server, do promote */
			elog(WARNING, "Could not establish connection");
			elog(WARNING, "The primary server might crashed");
			do_promote();
			break;
		}

		/* Polling */
		res = PQexec(con, "select 1");

		if (PQntuples(res) != 1)
		{
			/* If could not get tuple, do promote */
			elog(WARNING,"Could not get tuple");
			elog(WARNING, "The primary server might crashed");
			do_promote();
			break;
		}
		PQfinish(con);
	}
	proc_exit(1);
}

static pgpid_t
get_pgpid(void)
{
	FILE *pidf;
	long pid;

	pidf = fopen(pid_file, "r");

	if (pidf == NULL)
	{
		/* no pid file */
		elog(WARNING,"Could not open pid file : %s", pid_file);
		proc_exit(1);
	}

	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		elog(WARNING,"Could not scan pid from pid file : %s", pid_file);
		proc_exit(1);
	}

	fclose(pidf);

	return (pgpid_t) pid;
}

static void
do_promote(void)
{
	pgpid_t pid;
	FILE *prmfile;

	elog(LOG,"Do promote by pg_promoter");

	pid = get_pgpid();

	if (pid <= 0)
	{
		elog(WARNING, "Could not find pid file");
		proc_exit(1);
	}

    snprintf(promote_file, 1000, "%s/promote", pg_data);

	prmfile = fopen(promote_file, "w");
	if (prmfile == NULL)
	{
		elog(WARNING,"Could not create promote file");
		proc_exit(1);
	}
	if (fclose(prmfile))
	{
		elog(WARNING,"Could not close promote fils");
		proc_exit(1);
	}
	
	if (kill((pid_t) pid, SIGUSR1) != 0)
	{
		elog(WARNING," Failed to send SIGUSR1 signal to postmaster process : %d", (uint)pid);
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

	/* get the configuration */
	DefineCustomIntVariable("pg_promoter.keepalives",
							"Specific time between polling to primary server",
							NULL,
							&pg_promoter_keepalives,
							3,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("pg_promoter.primary_conninfo",
							"Connection information for primary server",
							NULL,
							&pg_promoter_primary_conninfo,
							"aaaa",
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
	worker.bgw_main = pg_promoter_main;
	worker.bgw_notify_pid = 0;
	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_promoter");
	worker.bgw_main_arg = Int32GetDatum(1);
	RegisterBackgroundWorker(&worker);
}
