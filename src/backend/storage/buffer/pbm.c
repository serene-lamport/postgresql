/*
 * Predictive Buffer Manager
 */
#include "postgres.h"

/* PBM includes */
#include "storage/pbm.h"
#include "storage/pbm/pbm_background.h"
#include "storage/pbm/pbm_internal.h"

/* Other files */
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/shmem.h"

// included last to avoid IDE complaining about unused imports...
#include "storage/buf_internals.h"
#include "access/parallel.h"
#include "access/heapam.h"
#include "catalog/index.h"
#include "lib/simplehash.h"

#include <time.h>

// TODO! look for ### comments --- low-priority/later TODOs
// TODO! look for DEBUGGING comments and disable/remove them once they definitely aren't needed



/* Global pointer to the single PBM */
PbmShared* pbm;


/*-------------------------------------------------------------------------
 * Prototypes for private methods
 *-------------------------------------------------------------------------
 */


// Shared logic of the public API methods (register/unregister/report position)
static inline BlockGroupHashEntry * RegisterInitBlockGroupEntries(BlockGroupHashKey * bgkey, BlockNumber nblocks);
static inline ScanHashEntry * RegisterCreateScanEntry(TableData * tbl, BlockNumber startblock, BlockNumber nblocks, ParallelBlockTableScanDesc parallelSeqScanData);
static inline void UnregisterDeleteScan(ScanId id, SharedScanStats stats);
struct scan_elem_allocation_state {
	BlockGroupScanListElem * new_stats;
	BlockNumber left_to_allocate;
};
static inline BlockGroupScanListElem * alloc_scan_list_elem(struct scan_elem_allocation_state * alloc_state);
static inline BlockNumber num_block_groups(BlockNumber nblocks);
static inline BlockNumber num_block_group_segments(BlockNumber nblocks);
static inline void update_scan_speed_estimate(unsigned long elapsed, uint32 blocks, ScanHashEntry * entry);


// get current time
static inline unsigned long get_time_ns(void);
static inline unsigned long get_timeslice(void);


// block group + count vector methods used by bitmap scans
static inline bgcnt_vec bcvec_init(void);
static inline void bcvec_free(bgcnt_vec * vec);
static 		  void bcvec_push_back(bgcnt_vec * vec, BlockNumber bg);
static inline void bcvec_inc_last(bgcnt_vec * vec);


// initialization for internal structs
static inline void InitSeqScanStatsEntry(BlockGroupScanListElem * temp, ScanId id, ScanHashEntry * sdata, BlockNumber bgnum);
static inline void InitBlockGroupData(BlockGroupData * data);
static inline void InitBitmapScanBlockGroupCountVec(struct BitmapHeapScanState * scan, bgcnt_vec * v);


// memory management for BlockGroupScanListElem
static inline BlockGroupScanListElem * try_get_bg_scan_elem(void);
static inline void free_bg_scan_elem(BlockGroupScanListElem *it);


// lookups in applicable hash maps
static inline ScanHashEntry * search_scan(ScanId id, HASHACTION action, bool* foundPtr);
static inline BlockGroupData * search_block_group(const BufferDesc * buf, bool* foundPtr);

static BlockGroupData * search_or_create_block_group(const BufferDesc * buf);


// block group iterator methods: for remembering position on block group map
static inline void bgit_init(pbm_bg_iterator * it, const BlockGroupHashKey * bgkey);
static inline BlockGroupHashEntry * bgit_advance_one(pbm_bg_iterator * it);
static inline BlockGroupData * bgit_advance_to(pbm_bg_iterator * it, BlockNumber bg);


// managing buffer <--> block group links
// this is most of the real work for the callbacks from freelist.c
static inline BlockGroupData * AddBufToBlockGroup(BufferDesc * buf);
static inline void RemoveBufFromBlockGroup(BufferDesc * buf);


// managing buffer priority
static inline unsigned long ScanTimeToNextConsumption(const BlockGroupScanListElem * bg_scan);

static unsigned long PageNextConsumption(BlockGroupData * bgdata, bool * requestedPtr);


// removing scans from block groups
static inline bool block_group_delete_scan(ScanId id, BlockGroupData * groupData);
static inline void
remove_seq_scan_from_range_circular(pbm_bg_iterator *bg_it, const ScanHashEntry * scan_entry, uint32 lo, uint32 hi);
static inline void remove_seq_scan_from_block_range(pbm_bg_iterator *bg_it, ScanId id, uint32 lo, uint32 hi);
static inline int
remove_bitmap_scan_from_block_range(const ScanId id, struct PBM_LocalBitmapScanState *const scan_state, const BlockNumber bg_hi);


// PQ methods
static inline void RefreshBlockGroup(BlockGroupData * data);
static inline void PQ_RefreshRequestedBuckets(void);


// debugging
static void debug_buffer_access(BufferDesc* buf, char* msg);
static void assert_scan_completely_unregistered(ScanHashEntry * scan);

#ifdef SANITY_PBM_BUFFERS
static void list_all_buffers(void);

// sanity checks
static void sanity_check_verify_block_group_buffers(const BufferDesc * buf);
#endif


/*-------------------------------------------------------------------------
 *  PBM initialization methods
 *-------------------------------------------------------------------------
 */

/*
 * Initialization of shared PBM data structures
 */
void InitPBM(void) {
	bool found;
	int hash_flags;
	HASHCTL hash_info;
	struct timespec ts;

	/* Create shared PBM struct */
	pbm = (PbmShared*) ShmemInitStruct("Predictive buffer manager", sizeof(PbmShared), &found);

	/* If the PBM was already initialized, nothing to do. */
	if (true == found) {
		Assert(IsUnderPostmaster);
		return;
	}

	/* Otherwise, ensure the PBM is only initialized in the postmaster */
	Assert(!IsUnderPostmaster);

	/* Initialize fields */
	pbm->next_id = 0;
	SpinLockInit(&pbm->scan_free_list_lock);
	slist_init(&pbm->bg_scan_free_list);
	pbm->initial_est_speed = 0.0001f;
// ### what should be initial-initial speed estimate lol
// ### need to adjust it for units...

	/* Record starting time */
	clock_gettime(PBM_CLOCK, &ts);
	pbm->start_time_sec = ts.tv_sec;

	/* Initialize map of scans */
	hash_info = (HASHCTL){
		.keysize = sizeof(ScanId),
		.entrysize = sizeof(ScanHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->ScanMap = ShmemInitHash("PBM active scan stats", 128, ScanMapMaxSize, &hash_info, hash_flags);

// ### make this partitioned! (HASH_PARTITION)
	/* Initialize map of block groups */
	hash_info = (HASHCTL) {
		.keysize = sizeof(BlockGroupHashKey),
		.entrysize = sizeof(BlockGroupHashEntry),
	};
	hash_flags = HASH_ELEM | HASH_BLOBS;
	pbm->BlockGroupMap = ShmemInitHash("PBM block group stats", 1024, BlockGroupMapMaxSize, &hash_info, hash_flags);

	/* Initialize the priority queue */
	pbm->BlockQueue = InitPbmPQ();
}

/*
 * Estimate size of PBM (including all shared structures)
 */
Size PbmShmemSize(void) {
	Size size = 0;
	size = add_size(size, sizeof(PbmShared));
	size = add_size(size, hash_estimate_size(ScanMapMaxSize, sizeof(ScanHashEntry)));
	size = add_size(size, hash_estimate_size(BlockGroupMapMaxSize, sizeof(BlockGroupHashEntry)));

	/*
	 * Assuming one scan per block in the database on average: (probably an underestimate?)
	 * 128 GiB = 2^37 B
	 * => 2^24 blocks (blk size = 2^13 B)
	 * => 2^19 groups (group size = 2^5 blocks for now)
	 * round to 2^20...
	 */
	size = add_size(size, sizeof(BlockGroupScanListElem) * (1 << 20));

	size = add_size(size, PbmPqShmemSize());

#ifdef TRACE_PBM
	{
		Size bytes = size % 1024;
		Size kb = (size / 1024) % 1024;
		Size mb = (size / 1024) / 1024;
		elog(INFO, "PBM shared mem estimated size (without extras): %lu bytes = %lu MiB, %lu KiB, %lu B"
			, size, mb, kb, bytes
		);
	}
#endif


	// actually estimate the size later... for now assume 100 MiB will be enough
	size = add_size(size, 100 << 6);
	return size;
}


/*-------------------------------------------------------------------------
 * Public API: Sequential scan methods
 *-------------------------------------------------------------------------
 */

/*
 * Setup data structures for a new sequential scan.
 */
void PBM_RegisterSeqScan(HeapScanDesc scan, struct ParallelContext *pctx) {
	ScanId id;
	ScanHashEntry * s_entry;
	TableData tbl;
	BlockGroupHashKey bgkey;
	BlockGroupHashEntry * bseg_first;
	BlockNumber startblock;
	BlockNumber nblocks, nblock_groups;
	struct scan_elem_allocation_state alloc_state;
	int bgnum;
	ParallelBlockTableScanDesc pscan = (ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;
	const bool isParallel = (pscan != NULL);

	/* Should not already be registered. */
	Assert(scan->pbmSharedScanData == NULL);

	/*
	 * Get stats from the scan.
	 *
	 * Parallel scans need special handling. We make sure to not *crash* or
	 * cause UB here, but note that parallel scans are not really supported
	 * right now...
	 */
	if (isParallel) {
		/* Get fields from the parallel scan data if applicable */
		startblock	= pscan->phs_startblock;
		nblocks		= pscan->phs_nblocks;
	} else {
		/* Non-parallel scan */
		startblock	= scan->rs_startblock;
		nblocks		= scan->rs_nblocks;
	}

	/* Sanity checks */
	Assert(startblock != InvalidBlockNumber);

	/* Compute ranges */
	nblock_groups = num_block_groups(nblocks);

	/* Don't bother registering if the table is small enough */
	if (nblock_groups <= 1) {
		return;
	}

	/* Keys for hash tables */
	tbl = (TableData){
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
	};

	bgkey = (BlockGroupHashKey) {
		.rnode = scan->rs_base.rs_rd->rd_node,
		.forkNum = MAIN_FORKNUM, // Sequential scans only use main fork
		.seg = 0,
	};

	/* Create a new entry for the scan */
	s_entry = RegisterCreateScanEntry(&tbl, startblock, nblocks, pscan);
	id = s_entry->id;

	/* Make sure every block group is present in the map! */
	bseg_first = RegisterInitBlockGroupEntries(&bgkey, nblocks);

	/*
	 * LOCKING: once we have created the entries, we no longer need to read or
	 * write the hash map so release the lock. We will iterate through the
	 * linked entries, but the relevant pointers will never change and individual
	 * block groups have separate concurrency control.
	 */

	/*
	 * Add the scan for each block group, then insert each block group into the
	 * PQ if applicable
	 */

	/* Refresh the PQ first if needed */
	PQ_RefreshRequestedBuckets();

	Assert(nblock_groups == 0 || NULL != bseg_first);
	Assert(nblock_groups == 0 || NULL == bseg_first->seg_prev);
	bgnum = 0;
	alloc_state = (struct scan_elem_allocation_state){
			.new_stats = NULL,
			.left_to_allocate = nblock_groups,
	};
	// Loop over block group segments
	for (BlockGroupHashEntry * bseg_cur = bseg_first; bgnum < nblock_groups; bseg_cur = bseg_cur->seg_next) {
		Assert(bseg_cur != NULL);

		// Loop over block groups within a segment
		for (int i = 0; i < BLOCK_GROUP_SEG_SIZE && bgnum < nblock_groups; ++bgnum, ++i) {
			BlockGroupData *const data = &bseg_cur->groups[i];
			BlockGroupScanListElem * scan_entry = NULL;

			/* Get an element for the block group scan list */
			scan_entry = alloc_scan_list_elem(&alloc_state);

			/* Initialize the list element & push to the list */
			InitSeqScanStatsEntry(scan_entry, id, s_entry, bgnum);

			/* Push the scan entry to the block group list */
			bg_lock_scans(data, LW_EXCLUSIVE);
			slist_push_head(&data->scans_list, &scan_entry->slist);
			bg_unlock_scans(data);

			/* Refresh the block group in the PQ if applicable */
			RefreshBlockGroup(data);
		}
	}

	/* Scan remembers the ID, shared stats, and local stats */
	scan->scanId = id;
	scan->pbmSharedScanData = s_entry;
	scan->pbmLocalScanStats = (LocalSeqScanStats) {
		.last_report_time = get_time_ns(),
		.last_pos = startblock,
	};
	/* Initialize block group iterator so we don't need hash lookups all the time. */
	bgit_init(&scan->pbmLocalScanStats.bg_it, &bgkey);
	scan->pbmLocalScanStats.bg_it.entry = bseg_first;

	if (isParallel) {
		pscan->pbmSharedScanData = s_entry;
		Assert(pctx != NULL);
		s_entry->data.pseq.nworkers = pctx->nworkers_to_launch + 1;
	}


	// debugging
#ifdef TRACE_PBM
	elog(INFO, "PBM_RegisterSeqScan(%lu): name=%s, nblocks=%d, num_blocks=%d, "
			   "startblock=%u, parallel=%s, scan=%p, shared_stats=%p",
		 id,
		 scan->rs_base.rs_rd->rd_rel->relname.data,
		 nblocks, 				// # of blocks in relation
		 scan->rs_numblocks, 	// max # of blocks, probably not set yet... (i.e. -1)
		 startblock,
		 (isParallel ? "true" : "false"),
		 scan, s_entry
	 );

#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#endif // TRACE_PBM
}

/*
 * Clean up after a sequential scan finishes.
 */
void PBM_UnregisterSeqScan(HeapScanDescData *scan) {
	const ScanId id = scan->scanId;
	ScanData scanData;
	uint32 upper, start, end;
	bool is_parallel;


	/* Nothing to do if not registered in the first place */
	if (NULL == scan->pbmSharedScanData) {
		return;
	}

	scanData = scan->pbmSharedScanData->data;
	is_parallel = (scan->rs_base.rs_parallel != NULL);

#ifdef TRACE_PBM
	elog(INFO, "PBM_UnregisterSeqScan(%lu) is_parallel=%s", id, (is_parallel ? "true" : "false"));
#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP
#endif // TRACE_PBM


	// Shift PQ buckets if needed
	PQ_RefreshRequestedBuckets();

	// For each block in the scan: remove it from the list of scans

	/* upper is the last possible block group for the scan, +1 since upper
	 * bound is exclusive */
	upper = (scanData.nblocks > 0 ? BLOCK_GROUP(scanData.nblocks - 1) + 1 : 0);
	end = (0 == scanData.startBlock ? upper : BLOCK_GROUP(scanData.startBlock));

	if (!is_parallel) {
		start = BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
		// Everything before `start` should already be removed when the scan passed that location
		// Everything from `start` (inclusive) to `end` (exclusive) needs to have the scan removed
		if (scanData.nblocks > 0) {
			remove_seq_scan_from_range_circular(&scan->pbmLocalScanStats.bg_it, scan->pbmSharedScanData, start, end);
		}
	} else {
		BlockNumber startBlock = scanData.startBlock;
		BlockNumber nblocks = scanData.nblocks;
		uint64 nalloced = scanData.pseq.nalloced;

#ifdef TRACE_PBM
		elog(INFO, "PBM_UnregisterSeqScan(%lu) nallocated=%lu, start=%u, nblocks=%u", id, nalloced, startBlock, nblocks);
#endif

		/*
		 * For parallel scans: remove anything that wasn't allocated to a worker
		 * Find the first un-allocated page (if applicable)
		 */
		if (nblocks > 0 && nalloced < nblocks) {
			pbm_bg_iterator it = {
				.entry = NULL,
				.bgkey = {
					.rnode = scanData.tbl.rnode,
					.forkNum = scanData.tbl.forkNum,
				},
			};

			start = (startBlock + nalloced) % nblocks;

			remove_seq_scan_from_range_circular(&it, scan->pbmSharedScanData, start, end);
		}
	}

	/* Sanity checks (controlled by SANITY_PBM_SCAN_FULLY_UNREGISTERED since this is expensive) */
	assert_scan_completely_unregistered(scan->pbmSharedScanData);

	// Remove from the scan map
	UnregisterDeleteScan(id, scanData.stats);

	// Make sure we don't try to unregister again
	scan->pbmSharedScanData = NULL;

#ifdef TRACE_PBM
	elog(INFO, "PBM_UnregisterSeqScan(%lu) finished", id);
#endif
}

/*
 * Update progress of a sequential scan.
 *
 * Note: this assumes we've already checked whether this should be done or not.
 */
void internal_PBM_ReportSeqScanPosition(struct HeapScanDescData * scan, BlockNumber pos) {
	unsigned long curTime, elapsed;
	ScanHashEntry *const entry = scan->pbmSharedScanData;
	BlockNumber blocks;
	const BlockNumber prevGroupPos	= BLOCK_GROUP(scan->pbmLocalScanStats.last_pos);
	const BlockNumber curGroupPos	= BLOCK_GROUP(pos);

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	/* Only trace calls which don't return immediately */
	elog(LOG, "PBM_ReportSeqScanPosition(%lu), pos=%u, group=%u", scan->scanId, pos, curGroupPos);
#endif

	/* Sanity checks */
	Assert(pos != InvalidBlockNumber);
	Assert(scan->rs_base.rs_parallel == NULL); /* Should not be called for parallel scans */
	Assert(entry != NULL);

	// Note: the entry is only *written* in one process.
	// If readers aren't atomic: how bad is this? Could mis-predict next access time...
	curTime = get_time_ns();
	elapsed = curTime - scan->pbmLocalScanStats.last_report_time;
	if (pos > scan->pbmLocalScanStats.last_pos) {
		blocks = pos - scan->pbmLocalScanStats.last_pos;
	} else {
		// looped around back to the start block
		blocks = pos + entry->data.nblocks - scan->pbmLocalScanStats.last_pos;
	}
	scan->pbmLocalScanStats.last_report_time = curTime;
	scan->pbmLocalScanStats.last_pos = pos;

	update_scan_speed_estimate(elapsed, blocks, entry);

	PQ_RefreshRequestedBuckets();

	// Remove the scan from blocks in range [prevGroupPos, curGroupPos)
	if (curGroupPos != prevGroupPos) {
		remove_seq_scan_from_range_circular(&scan->pbmLocalScanStats.bg_it, entry, prevGroupPos, curGroupPos);
	}

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	if (curGroupPos < 30 || curGroupPos % 64 == 0) {
		SharedScanStats stats = entry->data.stats;
		elog(INFO, "ReportSeqScanPosition(%lu) at block %d (group=%d), elapsed=%ld, blocks=%d, est_speed=%f",
			 scan->scanId, pos, BLOCK_GROUP(pos), elapsed, blocks, stats.est_speed);
	}
#endif


// ### maybe want to track whether scan is forwards or backwards... (not sure if relevant)
// ### ASSUMPTION: no backwards scans! (only for cursors anyways)
}

/*-------------------------------------------------------------------------
 * Public API: Parallel sequential scan methods (uses some of the same methods too)
 *-------------------------------------------------------------------------
 */

/*
 * Initialize parallel worker fields for sequential scans
 */
void PBM_InitParallelSeqScan(struct HeapScanDescData * scan, BlockNumber pos) {
	ParallelBlockTableScanWorker pwork = scan->rs_parallelworkerdata;
	bool is_leader = (scan->pbmSharedScanData != NULL);
	BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = scan->rs_base.rs_rd->rd_node,
			.forkNum = MAIN_FORKNUM,
			.seg = 0,
	};

	pwork->pbm_last_reported_pos = pos;
	pwork->pbm_last_report_time = get_time_ns();
	pwork->pbm_scanned_since_last_report = 0;

#ifdef TRACE_PBM
	elog(INFO, "PBM_InitParallelSeqScan! start_pos=%u, is_leader=%s, scan_shared=%p scanId=%ld"
		 , pos, (!is_leader ? "false" : "true")
		 , scan->pbmSharedScanData, scan->scanId
	);
#endif

	/* Everyone needs to initialize their local block group iterator. */
	bgit_init(&scan->pbmLocalScanStats.bg_it, &bgkey);

	/* Initialize chunk size if we are the leader */
	if (is_leader) {
// ### For now, we require the leader to participate since it updates the PBM stats.
		Assert(parallel_leader_participation);
		scan->pbmSharedScanData->data.pseq.chunk_size = scan->rs_parallelworkerdata->phsw_chunk_size;
	}
}

/*
 * Special handling for parallel sequential scans.
 *
 * Note: this assumes we've already checked whether this should be done or not.
 */
void internal_PBM_ParallelWorker_ReportSeqScanPosition(struct HeapScanDescData *scan, ParallelBlockTableScanDesc pbscan,
													   BlockNumber cur_page, BlockNumber new_page) {
	ParallelBlockTableScanWorkerData * pbscanwork = scan->rs_parallelworkerdata;
	BlockNumber last_reported, blocks_elapsed;
	bool is_leader = (scan->pbmSharedScanData != NULL);

	BlockNumber nblocks = pbscan->pbmSharedScanData->data.nblocks;
	BlockNumber last_in_rel = BLOCK_GROUP(nblocks - 1) + 1;
	BlockNumber lo, hi;

	unsigned long cur_time = get_time_ns();
	unsigned long t_elapsed = cur_time - pbscanwork->pbm_last_report_time;

	last_reported = pbscanwork->pbm_last_reported_pos;
	blocks_elapsed = ++pbscanwork->pbm_scanned_since_last_report;

	/* Update worker data */
	pbscanwork->pbm_last_report_time = cur_time;
	pbscanwork->pbm_last_reported_pos = new_page;
	pbscanwork->pbm_scanned_since_last_report = 0;

	/* Atomically update global # of blocks scanned */
	pbscan->pbm_nscanned += blocks_elapsed;

	/* Only update local speed if enough blocks have passed */
	if (blocks_elapsed >= 1 << PBM_BLOCK_GROUP_SHIFT) {
		float speed = (float)(blocks_elapsed) / (float)(t_elapsed);
		float old_speed = pbscanwork->pbm_worker_speed;

		/* Only update over-all speed if it changed at least 10% */
		float rel_diff = (old_speed == 0.0f ? 0.f : (speed - old_speed) / old_speed );
		if (old_speed == 0.0f || rel_diff > 0.1 || rel_diff < -0.1) {
			float delta_speed = speed - old_speed;
			pbscanwork->pbm_worker_speed = speed;

			/* Atomically update global speed. Global speed is sum of
			 * the workers, since they are processing in parallel */
			SpinLockAcquire(&pbscan->pbm_speed_lk);
			pbscan->pbm_est_scan_speed += delta_speed;
			SpinLockRelease(&pbscan->pbm_speed_lk);
		}
	}

	/*
	 * Leader should also: update global stats
	 */
	if (is_leader) {
		uint64 nalloced = pg_atomic_read_u64(&pbscan->phs_nallocated);

		SharedScanStats stats = {
			.est_speed = pbscan->pbm_est_scan_speed,
			.blocks_scanned = pbscan->pbm_nscanned,
		};
		if (stats.est_speed == 0.f) {
			stats.est_speed = pbm->initial_est_speed;
		}

		scan->pbmSharedScanData->data.stats = stats;

		/* Sequential-scan-only fields */
		scan->pbmSharedScanData->data.pseq.nalloced = nalloced;
		scan->pbmSharedScanData->data.pseq.chunk_size = pbscanwork->phsw_chunk_size;

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
		elog(INFO, "ReportParallelSeqScanPosition(%lu) (last=%u, cur=%u, next=%u) leader updating stats: "
				   "nalloced=%lu, speed=%f, scanned=%u"
			, scan->scanId, last_reported, cur_page, new_page
			, nalloced, stats.est_speed, stats.blocks_scanned
		);
#endif
	}

	PQ_RefreshRequestedBuckets();

	/*
	 * Unregister the scan from the block groups we have passed
	 *
	 * Want to remove: blocks [last_reported, cur_pos]
	 *
	 * We remove at the level of block *groups*, not blocks. It is possible
	 * for a group to be split between multiple "chunks" of the parallel
	 * scan, so it will be the first of one chunk and last of another.
	 *
	 * In this case, we want to remove when it is the last chunk, not first,
	 * since when we reach the last block it is likely whoever is responsible
	 * for the other part already did it since it was the start of their
	 * chunk.
	 *
	 * Use BLOCK_GROUP(cur_pos) + 1 for the end (since range is open on the
	 * right)
	 *
	 * For start: use BLOCK_GROUP(last_reported) IF last_reported is the
	 * 		first in the group, otherwise + 1 to leave that group for the
	 * 		other worker.
	 */

	lo = BLOCK_GROUP(last_reported);
	if (last_reported != GROUP_TO_FIRST_BLOCK(lo)) {
		lo += 1;

		/* handle wrap-around */
		if (lo >= last_in_rel) {
			lo = 0;
		}
	}

	/* hi + 1 because we want to remove inclusively */
	hi = BLOCK_GROUP(cur_page) + 1;

#if defined(TRACE_PBM) && defined(TRACE_PBM_REPORT_PROGRESS)
	elog(INFO, "ReportParallelSeqScanPosition(%lu) (last=%u, cur=%u, next=%u) from random worker, pscan=%p before removing scan, lo=%u, hi=%u"
		, pbscan->pbmSharedScanData->id, last_reported, cur_page, new_page, pbscan, lo, hi
	);
#endif

	remove_seq_scan_from_range_circular(&scan->pbmLocalScanStats.bg_it, pbscan->pbmSharedScanData, lo, hi);
}


/*-------------------------------------------------------------------------
 * Public API: BRIN methods
 *-------------------------------------------------------------------------
 */

/*
 * Setup data structures for tracking a bitmap scan.
 */
extern void PBM_RegisterBitmapScan(struct BitmapHeapScanState * scan) {
	ScanId id;
	ScanHashEntry * s_entry;
	TableData tbl;
	BlockGroupHashKey bgkey;
	BlockGroupHashEntry * bseg_first;
	BlockGroupHashEntry * bseg_cur;
	BlockNumber cnt;
	Relation rel = scan->ss.ss_currentRelation;
	bgcnt_vec v;
	struct scan_elem_allocation_state alloc_state;

	/* Need to know # of blocks. */
	const BlockNumber nblocks = RelationGetNumberOfBlocks(scan->ss.ss_currentRelation);

	/* Should not already be registered. */
	Assert(scan->pbmSharedScanData == NULL);

	/* Keys for hash tables */
	tbl = (TableData){
		.rnode = rel->rd_node,
		.forkNum = MAIN_FORKNUM, // Bitmap scans only use main fork (at least, `BitmapPrefetch` is hardcoded with MAIN_FORKNUM...)
	};

	bgkey = (BlockGroupHashKey) {
		.rnode = rel->rd_node,
		.forkNum = MAIN_FORKNUM, // Bitmap scans only use main fork (at least, `BitmapPrefetch` is hardcoded with MAIN_FORKNUM...)
		.seg = 0,
	};

	/* Make sure every block group is present in the map! */
	bseg_first = RegisterInitBlockGroupEntries(&bgkey, nblocks);

	/* Determine the block groups that will be scanned from the bitmap */
	InitBitmapScanBlockGroupCountVec(scan, &v);

	/* If nothing will be scanned (bitmap is empty), don't bother to register it */
	if (v.len == 0) {
		return;
	}

	/* Create a new entry for the scan */
	s_entry = RegisterCreateScanEntry(&tbl, 0, nblocks, NULL);
	id = s_entry->id;

	/* Refresh the PQ first if needed */
	PQ_RefreshRequestedBuckets();

	/*
	 * Add the scan for each block group which will be referenced.
	 */
	bseg_cur = bseg_first;
	cnt = 0;
	alloc_state = (struct scan_elem_allocation_state) {
		.new_stats = NULL,
		.left_to_allocate = v.len,
	};
	for (int i = 0; i < v.len; ++i) {
		BlockNumber cur_group = v.items[i].block_group;
		BlockNumber cur_group_seg = BLOCK_GROUP_SEGMENT(cur_group);
		BlockGroupData * data;
		BlockGroupScanListElem * scan_entry = NULL;

		/* Traverse forwards to the segment for the next block group */
		while (bseg_cur->key.seg < cur_group_seg) {
			bseg_cur = bseg_cur->seg_next;
		}

		/* The actual block group */
		data = &bseg_cur->groups[cur_group % BLOCK_GROUP_SEG_SIZE];

		/* Get an element for the block group scan list */
		scan_entry = alloc_scan_list_elem(&alloc_state);

		/*
		 * Initialize the scan stats entry. `blocks_behind` is the cumulative
		 * total of the counts in `v` so far.
		 */
		*scan_entry = (BlockGroupScanListElem) {
			.scan_id = id,
			.scan_entry = s_entry,
			.blocks_behind = cnt,
		};

		/* Push the scan entry to the block group list */
		bg_lock_scans(data, LW_EXCLUSIVE);
		slist_push_head(&data->scans_list, &scan_entry->slist);
		bg_unlock_scans(data);

		/* Refresh the block group in the PQ if applicable */
		RefreshBlockGroup(data);

		/* Track cumulative total */
		cnt += v.items[i].blk_cnt;
	}

	/* Remember the PBM data in the scan */
	scan->scanId = id;
	scan->pbmSharedScanData = s_entry;
	scan->pbmLocalScanData = (struct PBM_LocalBitmapScanState){
		.last_pos = 0,
		.last_report_time = get_time_ns(),
		.block_groups = v,
		.vec_idx = 0,
		.bg_it = {
				.entry = bseg_first,
				.bgkey = bgkey,
		},
	};

#ifdef TRACE_PBM
	elog(INFO, "PBM_RegisterBitmapScan(%lu): name=%s, nblocks=%d, "
			   "vec={len=%d, [{grp=%u, cnt=%u}, ..., {grp=%u, cnt=%u}]}",
		 id,
		 scan->ss.ss_currentRelation->rd_rel->relname.data,
		 nblocks,
		 v.len, v.items[0].block_group,			v.items[0].blk_cnt,
		 		v.items[v.len-1].block_group,	v.items[v.len-1].blk_cnt
	);

#ifdef TRACE_PBM_BITMAP_PROGRESS
	for (int i = 0; i < v.len; ++i) {
		elog(INFO, "PBM_RegisterBitmapScan(%lu) bitmap block_group=%u, cnt=%u",
			 id, v.items[i].block_group, v.items[i].blk_cnt
		);
	}
#endif // TRACE_PBM_BITMAP_PROGRESS

#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP

#ifdef TRACE_PBM_PRINT_BLOCKGROUPMAP
	debug_log_blockgroup_map();
#endif // TRACE_PBM_PRINT_BLOCKGROUPMAP
#endif /* TRACE_PBM */
}

/*
 * Clean up after a bitmap scan finishes.
 */
extern void PBM_UnregisterBitmapScan(struct BitmapHeapScanState * scan, char* msg) {
	const ScanId id = scan->scanId;
	const int vec_idx = scan->pbmLocalScanData.vec_idx;
	const int vec_len = scan->pbmLocalScanData.block_groups.len;
	uint32 upper;
	ScanData scanData;

	/* Nothing to do if there is no scan registered. */
	if (NULL == scan->pbmSharedScanData) {
		return;
	}

#if defined(TRACE_PBM)
	elog(INFO, "PBM_UnregisterBitmapScan(%lu)! %s   do_anything=%s",
		 id, msg, (scan->pbmSharedScanData != NULL ? "true" : "false"));
#endif // TRACE_PBM

	scanData = scan->pbmSharedScanData->data;

	/* Shift PQ buckets if needed */
	PQ_RefreshRequestedBuckets();

	/* Remove from the rest of the block groups, unless there are none */
	if (scan->pbmLocalScanData.block_groups.len > 0 && vec_idx < vec_len) {
		/* Delete the scan from relevant range */
		upper = (scanData.nblocks > 0 ? BLOCK_GROUP(scanData.nblocks - 1) + 1 : 0);
		remove_bitmap_scan_from_block_range(id, &scan->pbmLocalScanData, upper);

		/* Remove from the scan map */
		UnregisterDeleteScan(id, scanData.stats);
	}

	/* After deleting the scan, unlink from the scan state so it doesn't try to end the scan again */
	scan->pbmSharedScanData = NULL;
	bcvec_free(&scan->pbmLocalScanData.block_groups);
// ### do we want to keep the block_groups vec in case of rescan? how to detect this?

#ifdef TRACE_PBM
#ifdef TRACE_PBM_PRINT_SCANMAP
	debug_log_scan_map();
#endif // TRACE_PBM_PRINT_SCANMAP

#ifdef TRACE_PBM_PRINT_BLOCKGROUPMAP
	debug_log_blockgroup_map();
#endif // TRACE_PBM_PRINT_BLOCKGROUPMAP
#endif /* TRACE_PBM */
}

/*
 * Update progress of a bitmap scan.
 */
extern void internal_PBM_ReportBitmapScanPosition(struct BitmapHeapScanState *const scan, const BlockNumber pos) {
	const ScanId id = scan->scanId;
	unsigned long curTime, elapsed;
	ScanHashEntry *const scan_entry = scan->pbmSharedScanData;
	bgcnt_vec *const v = &scan->pbmLocalScanData.block_groups;
	int i = scan->pbmLocalScanData.vec_idx;
	const BlockNumber bg = BLOCK_GROUP(pos);
	BlockNumber cnt = 0;

#if defined(TRACE_PBM) && defined (TRACE_PBM_BITMAP_PROGRESS)
	elog(INFO, "PBM_ReportBitmapScanPosition(%lu): pos=%u bg=%u   is_registered=%s",
		 id, pos, bg, (scan->pbmSharedScanData != NULL ? "true" : "false"));
#endif // TRACE_PBM && TRACE_PBM_BITMAP_PROGRESS

	/* This is the first time reporting progress, and we haven't actually done
	 * anything yet so SKIP
	 *
	 * Note: this is an uncommon case so we don't bother to check it in pbm_inline.h
	 */
	if (v->len == 0 || bg == v->items[0].block_group) {
		Assert(0 == i);
		return;
	}

	/* Sanity checks */
	Assert(pos != InvalidBlockNumber);
	Assert(bg > v->items[i].block_group);
	Assert(scan_entry != NULL);

	/* Update processing speed estimates. */
	curTime = get_time_ns();
	elapsed = curTime - scan->pbmLocalScanData.last_report_time;
	cnt = 0;
	/* Count number of blocks that have been passed */
	while (bg > v->items[i].block_group) {
		cnt += v->items[i].blk_cnt;
		i += 1;
	}
	update_scan_speed_estimate(elapsed, cnt, scan_entry);

	/* Update data stored locally in the scan. */
	scan->pbmLocalScanData.last_report_time = curTime;
	scan->pbmLocalScanData.last_pos = pos;


	PQ_RefreshRequestedBuckets();

	/* Remove the scan reference from the processed block group(s) and update index */
	i = remove_bitmap_scan_from_block_range(id, &scan->pbmLocalScanData, bg);
	scan->pbmLocalScanData.vec_idx = i;
}


/*-------------------------------------------------------------------------
 * Public API: Tracking buffers
 *-------------------------------------------------------------------------
 */

/*
 * Notify the PBM about a new buffer so it can be added to the priority queue
 */
void PbmNewBuffer(BufferDesc * const buf) {
	BlockGroupData* group;

#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	elog(WARNING, "PbmNewBuffer added new buffer:" //"\n"
			   "\tnew={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u (%u) %d/%d}",
		 buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode, buf->tag.blockNum,
		 BLOCK_GROUP(buf->tag.blockNum), buf->pbm_bgroup_next, buf->pbm_bgroup_prev
	);
	debug_buffer_access(buf, "new buffer");
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_NEW

	// Buffer must not already be in a block group if it is new!
	Assert(FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_prev);
	Assert(FREENEXT_NOT_IN_LIST == buf->pbm_bgroup_next);

	group = AddBufToBlockGroup(buf);

	// There must be a group -- either it already existed or we created it.
	Assert(group != NULL);

#ifdef SANITY_PBM_BUFFERS
	sanity_check_verify_block_group_buffers(buf);
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_NEW)
	BufferDesc* temp = GetBufferDescriptor(group->buffers_head);
	BufferDesc* temp2 = temp->pbm_bgroup_next < 0 ? NULL : GetBufferDescriptor(temp->pbm_bgroup_next);

	if (temp2 != NULL) {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
				   "\n\t old={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}",
			 temp->buf_id, temp->tag.rnode.spcNode, temp->tag.rnode.dbNode, temp->tag.rnode.relNode,
			 temp->tag.blockNum, BLOCK_GROUP(temp->tag.blockNum), temp->pbm_bgroup_prev, temp->pbm_bgroup_next
			 , temp2->buf_id, temp2->tag.rnode.spcNode, temp2->tag.rnode.dbNode, temp2->tag.rnode.relNode,
			 temp2->tag.blockNum, BLOCK_GROUP(temp2->tag.blockNum), temp2->pbm_bgroup_prev, temp2->pbm_bgroup_next
		);
	} else {
		elog(INFO, "PbmNewBuffer added new buffer:"
				   "\n\t new={id=%d, tbl={spc=%u, db=%u, rel=%u} block=%u group=%u prev=%d next=%d}"
			 	   "\n\t old={n/a}",
			 temp->buf_id, temp->tag.rnode.spcNode, temp->tag.rnode.dbNode, temp->tag.rnode.relNode,
			 temp->tag.blockNum, BLOCK_GROUP(temp->tag.blockNum), temp->pbm_bgroup_next, temp->pbm_bgroup_prev
		);
	}
#endif // tracing
#endif // SANITY_PBM_BUFFERS

/*
 * ### Consider doing this unconditionally.
 * Pros: might get better estimates with more frequent updates
 * Const: for seq scans: we're refreshing several times in a row uselessly, more overhead.
 */
	// Push the bucket to the PQ if it isn't already there
	if (NULL == group->pq_bucket) {
		RefreshBlockGroup(group);
	}
}

/*
 * Notify the PBM when we *remove* a buffer to keep data structure up to date.
 */
void PbmOnEvictBuffer(struct BufferDesc *const buf) {
#if defined(TRACE_PBM) && defined(TRACE_PBM_BUFFERS) && defined(TRACE_PBM_BUFFERS_EVICT)
	static int num_evicted = 0;
	elog(WARNING, "evicting buffer %d tbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u, #evictions=%d",
		 buf->buf_id, buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode,
		 buf->tag.forkNum, buf->tag.blockNum, num_evicted++);
#endif // TRACE_PBM && TRACE_PBM_BUFFERS && TRACE_PBM_BUFFERS_EVICT

	// Nothing to do if we aren't actually evicting anything
	if (buf->tag.blockNum == InvalidBlockNumber) {
		return;
	}

#ifdef SANITY_PBM_BUFFERS
	// Check everything in the block group actually belongs to the same group
	sanity_check_verify_block_group_buffers(buf);
#endif // SANITY_PBM_BUFFERS

	RemoveBufFromBlockGroup(buf);

	Assert(buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST);
	Assert(buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST);
}


/*-------------------------------------------------------------------------
 * Public API: Maintenance methods called in the background
 *-------------------------------------------------------------------------
 */

/*
 * Shift buckets in the PBM PQ as necessary IF the lock can be acquired without
 * waiting.
 * If someone else is actively using the queue for anything, then do nothing.
 */
void PBM_TryRefreshRequestedBuckets(void) {
	unsigned long ts = get_timeslice();
	unsigned long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
	bool up_to_date = (last_shifted_ts + 1 > ts);
	bool acquired;

#if defined(TRACE_PBM) && defined(TRACE_PBM_PQ_REFRESH)
	elog(INFO, "PBM try refresh buckets: t=%ld, last=%ld, up_to_date=%s",
		 ts, last_shifted_ts, up_to_date?"true":"false");
#endif // TRACE_PBM_PQ_REFRESH

	// Nothing to do if already up to date
	if (up_to_date) return;

	// If unable to acquire the lock, just stop here
	acquired = LWLockConditionalAcquire(PbmPqBucketsLock, LW_EXCLUSIVE);
	if (acquired) {

		// if several time slices have passed since last shift, try to short-circuit by
		// checking if the whole PQ is empty, in which case we can just update the timestamp without actually shifting anything
		if ((ts - last_shifted_ts) > 5) {
			if (PQ_CheckEmpty()) {
				pbm->BlockQueue->last_shifted_time_slice = ts;
			}
		}

		// Shift buckets until up-to-date
		while (PQ_ShiftBucketsWithLock(ts)) ;

		LWLockRelease(PbmPqBucketsLock);
	} else {
		return;
	}
}


/*-------------------------------------------------------------------------
 * Private helpers:
 *-------------------------------------------------------------------------
 */

/*
 * The main logic for Register*Scan to create block group entries if necessary.
 */
BlockGroupHashEntry * RegisterInitBlockGroupEntries(BlockGroupHashKey *const bgkey, const BlockNumber nblocks) {
	bool found;
	BlockGroupHashEntry * bseg_first = NULL;
	BlockGroupHashEntry * bseg_prev = NULL;
	BlockGroupHashEntry * bseg_cur;
	const BlockNumber nblock_segs = num_block_group_segments(nblocks);

	/* Make sure every block group is present in the map! */
	LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
		// For each segment, create entry in the buffer map if it doesn't exist already

		for (BlockNumber seg = 0; seg < nblock_segs; ++seg) {
			bgkey->seg = seg;

			// Look up the next segment (or create it) in the hash table if necessary
			if (NULL == bseg_prev || NULL == bseg_prev->seg_next) {
				bseg_cur = hash_search(pbm->BlockGroupMap, bgkey, HASH_ENTER, &found);

				// if created a new entry, initialize it!
				if (!found) {
					bseg_cur->seg_next = NULL;
					bseg_cur->seg_prev = bseg_prev;
					for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
						/* LOCKING: the new entry is protected by PbmBlocksLock,
						 * not accessible without the hash table until we link
						 * to the previous entry.
						 */
						InitBlockGroupData(&bseg_cur->groups[i]);
					}
				}

				/*
				 * Link the previous segment to the current one.
				 *
				 * Locking: Do this AFTER initializing all the block groups!
				 * So that someone else traversing the in-order links can't
				 * find this before the block groups are initialized. (someone
				 * searching the map won't find it, because we have an exclusive
				 * lock on the map)
				 */
				if (bseg_prev != NULL) {
					// the links should only be initialized once, ever
					Assert(bseg_prev->seg_next == NULL);
					Assert(!found || bseg_cur->seg_prev == NULL);

					// link to previous if applicable
					bseg_prev->seg_next = bseg_cur;
					bseg_cur->seg_prev = bseg_prev;
				} else {
					Assert(NULL == bseg_first);
					// remember the *first* segment if there was no previous
					bseg_first = bseg_cur;
				}
			} else {
				bseg_cur = bseg_prev->seg_next;
			}

			// remember current segment as previous for next iteration
			bseg_prev = bseg_cur;
		}
	} // LOCK_GUARD

	return bseg_first;
}

/*
 * The main logic for Register*Scan to create the new scan metadata entry.
 */
ScanHashEntry *RegisterCreateScanEntry(TableData *const tbl, const BlockNumber startblock, const BlockNumber nblocks, ParallelBlockTableScanDesc parallelSeqScanData) {
	ScanId id;
	ScanHashEntry * s_entry;
	bool found;
	const float init_est_speed = pbm->initial_est_speed;

	/* Insert the scan metadata & generate scan ID */
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		// Generate scan ID
		id = pbm->next_id;
		pbm->next_id += 1;

		// Create s_entry for the scan in the hash map
		s_entry = search_scan(id, HASH_ENTER, &found);
		Assert(!found); // should be inserted...
	}

	/*
	 * Initialize the entry. It is OK to do this outside the lock, because we
	 * haven't associated the scan with any block groups yet (so no one will by
	 * trying to access it yet.
	 */
	s_entry->data = (ScanData) {
		// These fields never change
		.tbl = (*tbl),
		.startBlock = startblock,
		.nblocks = nblocks,
		// These stats will be updated later
		.stats = (SharedScanStats) {
			.est_speed = init_est_speed,
			.blocks_scanned = 0,
		},
		// parallel seq scan fields
		.pbscan = parallelSeqScanData,
		.pseq = {
			.nalloced = 0,
			.chunk_size = 0,
		},
	};

	return s_entry;
}

/*
 * Remove a scan from the scan map on Unregister*
 */
void UnregisterDeleteScan(const ScanId id, const SharedScanStats stats) {
	// Remove the scan metadata from the map & update global stats while we have the lock
	LOCK_GUARD_V2(PbmScansLock, LW_EXCLUSIVE) {
		const float alpha = 0.85f; // ### pick something else? move to configuration?
		float new_est;
		bool found;

		// Delete the scan from the map (should be found!)
		search_scan(id, HASH_REMOVE, &found);
		Assert(found);

		/* Only update the global speed estimate if the amount scanned is enough
		 * to have a speed estimate in the first place */
		if (stats.blocks_scanned > (1 << PBM_BLOCK_GROUP_SHIFT)) {
			// Update global initial speed estimate: geometrically-weighted average
			new_est = pbm->initial_est_speed * alpha + stats.est_speed * (1.f - alpha);
			pbm->initial_est_speed = new_est;
		}
	}
}

/* Helper for allocating block group scan elements when registering scans */
BlockGroupScanListElem * alloc_scan_list_elem(struct scan_elem_allocation_state * alloc_state) {
	BlockGroupScanListElem * ret = NULL;

	/* Get an element for the block group scan list */
	if (NULL != alloc_state->new_stats) {
		/* We've already allocated for these block groups, use the allocated stuff... */
		ret = alloc_state->new_stats;
		alloc_state->new_stats += 1;
	} else {
		/* Try to allocate from the free list */
		ret = try_get_bg_scan_elem();

		/* If nothing free, allocate enough for everything else */
		if (NULL == ret) {
			alloc_state->new_stats = ShmemAlloc(alloc_state->left_to_allocate * sizeof(BlockGroupScanListElem));
			ret = alloc_state->new_stats;
			alloc_state->new_stats += 1;
		}
	}

	Assert(alloc_state->left_to_allocate > 0);
	alloc_state->left_to_allocate -= 1;
	return ret;
}

BlockNumber num_block_groups(BlockNumber nblocks) {
	BlockNumber last_block 			= (nblocks == 0 ? 0 : nblocks - 1);
	BlockNumber last_block_group	= BLOCK_GROUP(last_block);
	BlockNumber nblock_groups 		= (nblocks == 0 ? 0 : last_block_group + 1);

	return nblock_groups;
}

BlockNumber num_block_group_segments(BlockNumber nblocks) {
	BlockNumber last_block = (nblocks == 0 ? 0 : nblocks - 1);
	BlockNumber last_block_group = BLOCK_GROUP(last_block);
	BlockNumber last_block_seg = BLOCK_GROUP_SEGMENT(last_block_group);
	BlockNumber nblock_segs = (nblocks == 0 ? 0 : last_block_seg + 1);

	return nblock_segs;
}

static inline void update_scan_speed_estimate(unsigned long elapsed, uint32 blocks, ScanHashEntry * entry) {
	float speed = (float)(blocks) / (float)(elapsed);
	SharedScanStats stats = entry->data.stats;

// ### estimating speed: should do better than this. e.g. exponentially weighted average, or moving average.

	/* update shared stats with a single assignment */
	stats.blocks_scanned += blocks;
	stats.est_speed = speed;
	entry->data.stats = stats;
}

/* Current time in nanoseconds */
unsigned long get_time_ns(void) {
	struct timespec now;
	clock_gettime(PBM_CLOCK, &now);

	return NS_PER_SEC * (now.tv_sec - pbm->start_time_sec) + now.tv_nsec;
}

/* Current time slice for the PQ */
unsigned long get_timeslice(void) {
	return ns_to_timeslice(get_time_ns());
}

/* Create an empty bcvec */
bgcnt_vec bcvec_init(void) {
	int cap = 4;
	return (bgcnt_vec) {
			.len = 0,
			.cap = cap,
			.items = palloc(sizeof(struct bg_ct_pair) * cap),
	};
}

void bcvec_free(bgcnt_vec * vec) {
	pfree(vec->items);
}

/* Puch the given block group to the end of the bcvec with count 1 */
void bcvec_push_back(bgcnt_vec * vec, BlockNumber bg) {

	// Grow if needed
	if (vec->len >= vec->cap) {
		int new_cap = vec->cap * 2;

		struct bg_ct_pair * new_items = palloc(sizeof(struct bg_ct_pair) * new_cap);

		for (int i = 0; i < vec->len; ++i) {
			new_items[i] = vec->items[i];
		}

		pfree(vec->items);

		vec->cap = new_cap;
		vec->items = new_items;
	}

	// Insert the new item
	vec->items[vec->len] = (struct bg_ct_pair) {
			.block_group = bg,
			.blk_cnt = 1,
	};

	vec->len += 1;
}

/* Increment the count of the last item by 1 */
void bcvec_inc_last(bgcnt_vec * vec) {
	vec->items[vec->len - 1].blk_cnt += 1;
}

/* Initialize an entry in the list of scans for a block group, for a sequential scan. */
void InitSeqScanStatsEntry(BlockGroupScanListElem *temp, ScanId id, ScanHashEntry *sdata, BlockNumber bgnum) {
	const BlockNumber startblock = sdata->data.startBlock;
	const BlockNumber nblocks = sdata->data.nblocks;
	// convert group # -> block #
	const BlockNumber first_block_in_group = GROUP_TO_FIRST_BLOCK(bgnum);
	BlockNumber blocks_behind;

	// calculate where the block group is in the scan relative to start block
	if (first_block_in_group >= startblock) {
		// Normal case: no circular scans or we have not wrapped around yet
		blocks_behind = first_block_in_group - startblock;
	} else {
		// Circular scans: eventually we loop back to the start "before" the start block, have to adjust
		blocks_behind = first_block_in_group + nblocks - startblock;

		/*
		 * Special case: if this is the group of the start block but startblock
		 * is NOT the first in the group, then this group is both at the start
		 * and the end. For out purposes treat it as the end, since we will
		 * access it at the start immediately and it is either in cache or not,
		 * very unlikely that tracking that information would matter.
		 *
		 * (this is automatically handled by this case)
		 */
	}

	// fill in data of the new list element
	*temp = (BlockGroupScanListElem){
			.scan_id = id,
			.scan_entry = sdata,
			.blocks_behind = blocks_behind,
	};
}

/* Initialize metadata for a block group */
void InitBlockGroupData(BlockGroupData * data) {
	slist_init(&data->scans_list);
	data->buffers_head = FREENEXT_END_OF_LIST;
	data->pq_bucket = NULL;

	// Initialize locks for the block group
#if PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_LWLOCK
	LWLockInitialize(&data->lock, LWTRANCHE_PBM_BLOCK_GROUP);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_SINGLE_SPIN
	SpinLockInit(&data->slock);
#elif PBM_BG_LOCK_MODE == PBM_BG_LOCK_MODE_DOUBLE_SPIN
	SpinLockInit(&data->scan_lock);
	SpinLockInit(&data->buf_lock);
#endif // PBM_BG_LOCK_MODE
}

/*
 * Find the block groups (& number of blocks in each group) that will be seen
 * by a bitmap scan.
 */
void InitBitmapScanBlockGroupCountVec(struct BitmapHeapScanState *scan, bgcnt_vec *v) {
	TIDBitmap * tbm = scan->tbm;
	TBMIterator * it_private;
	dsa_pointer it_shared_dsa;
	dsa_area * dsa = scan->ss.ps.state->es_query_dsa;
	TBMSharedIterator * it_shared;
	TBMIterateResult * res = NULL;
	BlockNumber prev_bg = InvalidBlockNumber;
	bool parallel = (NULL != scan->pstate);
	BlockNumber bg;

	*v = bcvec_init();

	/* Depending on whether the scan is parallel, start iterating the bitmap
	 * either privately or shared. */
	if (!parallel) {
		// ### consider an "iterate_lossy" method which only outputs block #s, ignoring the tuple offsets
		it_private = tbm_begin_iterate(tbm);
	} else {
		it_shared_dsa = tbm_prepare_shared_iterate(tbm);
		it_shared = tbm_attach_shared_iterate(dsa, it_shared_dsa);
	}

	for (;;) {
		/* Next block */
		if (!parallel) {
			res = tbm_iterate(it_private);
		} else {
			res = tbm_shared_iterate(it_shared);
		}

		/* Stop when the iterator ends */
		if (NULL == res) break;

		/* Check block group of next block */
		bg = BLOCK_GROUP(res->blockno);

		if (bg != prev_bg) {
			bcvec_push_back(v, bg);
			prev_bg = bg;
		} else {
			bcvec_inc_last(v);
		}
	}

	if (!parallel) {
		tbm_end_iterate(it_private);
	} else {
		tbm_end_shared_iterate(it_shared);
	}
}

/* Pop something of the scan stats free list. Returns NULL if it is empty */
BlockGroupScanListElem * try_get_bg_scan_elem(void) {
	BlockGroupScanListElem * ret = NULL;

	if (slist_is_empty(&pbm->bg_scan_free_list)) {
		return NULL;
	}

	SpinLockAcquire(&pbm->scan_free_list_lock);
	if (!slist_is_empty(&pbm->bg_scan_free_list)) {
		slist_node * snode = slist_pop_head_node(&pbm->bg_scan_free_list);
		ret = slist_container(BlockGroupScanListElem, slist, snode);
	}
	SpinLockRelease(&pbm->scan_free_list_lock);

	return ret;
}

/* Put the given scan list element onto the free list.
 * Shared memory can't be properly "freed" so we need to reuse it ourselves */
void free_bg_scan_elem(BlockGroupScanListElem *it) {
	SpinLockAcquire(&pbm->scan_free_list_lock);
	slist_push_head(&pbm->bg_scan_free_list, &it->slist);
	SpinLockRelease(&pbm->scan_free_list_lock);
}

/*
 * Search scan map for the given scan by its ID.
 *
 * Requires PqmScansLock to already be held as exclusive for insert/delete, at least shared for reads
 */
ScanHashEntry * search_scan(const ScanId id, HASHACTION action, bool* foundPtr) {
	return ((ScanHashEntry*)hash_search(pbm->ScanMap, &id, action, foundPtr));
}

/*
 * Find the data in the PBM corresponding to the given buffer, if we are tracking it.
 *
 * This acquires PbmBlocksLock internally for the hash lookup.
 */
BlockGroupData * search_block_group(const BufferDesc *const buf, bool* foundPtr) {
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
			.seg = BLOCK_GROUP_SEGMENT(bgroup),
	};
	BlockGroupHashEntry * bg_entry;

	// Look up the block group
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		bg_entry = hash_search(pbm->BlockGroupMap, &bgkey, HASH_FIND, foundPtr);
	}

	if (false == *foundPtr) {
		return NULL;
	} else {
		return &bg_entry->groups[bgroup % BLOCK_GROUP_SEG_SIZE];
	}
}

/*
 * Find the block group for the given buffer, or create it if it doesn't exist.
 *
 * This internally acquires PbmBlocksLock in the required mode when searching the hash table.
 */
BlockGroupData * search_or_create_block_group(const BufferDesc *const buf) {
	bool found;
	BlockGroupHashEntry * bg_entry;
	const BlockNumber blockNum = buf->tag.blockNum;
	const BlockNumber bgroup = BLOCK_GROUP(blockNum);
	const BlockGroupHashKey bgkey = (BlockGroupHashKey) {
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
			.seg = BLOCK_GROUP_SEGMENT(bgroup),
	};

	// Pre-compute the hash since we may do 2 lookups
	uint32 key_hash = get_hash_value(pbm->BlockGroupMap, &bgkey);

	// Search in the hash map with the lock
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		bg_entry = hash_search_with_hash_value(pbm->BlockGroupMap, &bgkey, key_hash, HASH_FIND, &found);
	}

	// If we didn't find the entry, create one
	if (!found) {
		LOCK_GUARD_V2(PbmBlocksLock, LW_EXCLUSIVE) {
			bg_entry = hash_search_with_hash_value(pbm->BlockGroupMap, &bgkey, key_hash, HASH_ENTER, &found);

			// There is a chance someone else created it at the same time and got to it first
			// If not, initialize the block map entry
			if (!found) {
				// Initialize the block groups
				for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
					InitBlockGroupData(&bg_entry->groups[i]);
				}

				// Don't link it to adjacent segments - this will get fixed lazily by sequential scans
				bg_entry->seg_next = NULL;
				bg_entry->seg_prev = NULL;
			}
		}
	}

	// Find the block group within the segment
	return &bg_entry->groups[bgroup % BLOCK_GROUP_SEG_SIZE];
}

/* Initialize a block group iterator for the table given in bgkey */
void bgit_init(pbm_bg_iterator * it, const BlockGroupHashKey *const bgkey) {
	it->bgkey = *bgkey;
	it->entry = NULL;
}

/*
 * Advance a block group iterator to the next *segment*.
 * Requires the iterator to already have looked up the entry.
 */
BlockGroupHashEntry * bgit_advance_one(pbm_bg_iterator *const it) {
	BlockNumber seg;

	Assert(it->entry != NULL);

	seg = it->entry->key.seg;
	it->entry = it->entry->seg_next;

	/* Sanity check: either we're at the end or the segment increased by 1 */
	Assert(NULL == it->entry || it->entry->key.seg == seg + 1);

	return it->entry;
}

BlockGroupData * bgit_advance_to(pbm_bg_iterator *const it, const BlockNumber bg) {
	BlockNumber seg = BLOCK_GROUP_SEGMENT(bg);
	BlockNumber seg_offset = bg % BLOCK_GROUP_SEG_SIZE;

	/* If we haven't looked up the entry yet or it would need wrapping around,
	 * do the lookup now. */
	if (NULL == it->entry || seg < it->entry->key.seg) {
		bool found;

		it->bgkey.seg = seg;
		LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
			it->entry = hash_search(pbm->BlockGroupMap, &it->bgkey, HASH_FIND, &found);
		}

		/* DEBUGGING: this got an error during `create index`
		 * (maybe can remove this -- was moved from `remove_seq_scan_from_block_range`)
		 */
		if (!found) {
			PBM_DEBUG_print_pbm_state();
			PBM_DEBUG_sanity_check_buffers();

			elog(WARNING, "bgit_advance_to(bg=%u) key={rnode={spc=%u, db=%u, rel=%u}, fork=%d, seg=%u}",
				 bg,
				 it->bgkey.rnode.spcNode, it->bgkey.rnode.dbNode, it->bgkey.rnode.relNode,
				 it->bgkey.forkNum, it->bgkey.seg
			);
		}
		Assert(found);
	}

	Assert(seg >= it->entry->key.seg);

	/* Advance to the appropriate segment if necessary */
	while (it->entry->key.seg < seg) {
		bgit_advance_one(it);
	}

	/* Once we have the right segment, return the relevant group from it. */
	return &it->entry->groups[seg_offset];
}

/*
 * Link the given buffer in to the associated block group.
 * Buffer must not already be part of any group.
 *
 * Caller is responsible for pushing the block group to the PBM PQ if necessary.
 */
BlockGroupData * AddBufToBlockGroup(BufferDesc *const buf) {
	BlockGroupData * group;
	Buffer group_head;

	// Buffer must not already be in a block group
	Assert(buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST
		   && buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST);

	// Find or create the block group
	group = search_or_create_block_group(buf);
	Assert(group != NULL);

	// Lock block group for insert
	bg_lock_buffers(group, LW_EXCLUSIVE);

	// Link the buffer into the block group chain of buffers
	group_head = group->buffers_head;
	group->buffers_head = buf->buf_id;
	buf->pbm_bgroup_next = group_head;
	buf->pbm_bgroup_prev = FREENEXT_END_OF_LIST;
	if (group_head != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(group_head)->pbm_bgroup_prev = buf->buf_id;
	}

	bg_unlock_buffers(group);

	// If this is the first buffer in the block group, caller will add it to the PQ
	return group;
}

/*
 * Remove a buffer from its block group, and if the block group is now empty
 * remove it from the PQ as well.
 *
 * Needs: `buf` should be a valid shared buffer, and therefore must already be
 * in a block somewhere.
 */
void RemoveBufFromBlockGroup(BufferDesc *const buf) {
	int next, prev;
	bool found;
	BlockGroupData * group;
	bool need_to_remove;

	/*
	 * DEBUGGING: got this error once while dropping the *last* index on the table.
	 *
	 * Not sure why.
	 *
	 * ### remove debugging check eventually.
	 */
	if (buf->pbm_bgroup_next == FREENEXT_NOT_IN_LIST || buf->pbm_bgroup_prev == FREENEXT_NOT_IN_LIST) {
		debug_buffer_access(buf, "remove_buf_from_block_group, buffer is not in a block group!");

		PBM_DEBUG_print_pbm_state();
		PBM_DEBUG_sanity_check_buffers();
	}

	// This should never be called for a buffer which isn't in the list
	Assert(buf->pbm_bgroup_next != FREENEXT_NOT_IN_LIST);
	Assert(buf->pbm_bgroup_prev != FREENEXT_NOT_IN_LIST);

	// Need to find and lock the block group before doing anything
	group = search_block_group(buf, &found);
	Assert(found);
	bg_lock_buffers(group, LW_EXCLUSIVE);

	next = buf->pbm_bgroup_next;
	prev = buf->pbm_bgroup_prev;

	// Unlink from neighbours
	buf->pbm_bgroup_prev = FREENEXT_NOT_IN_LIST;
	buf->pbm_bgroup_next = FREENEXT_NOT_IN_LIST;

	// unlink first if needed
	if (next != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(next)->pbm_bgroup_prev = prev;
	}
	if (prev != FREENEXT_END_OF_LIST) {
		GetBufferDescriptor(prev)->pbm_bgroup_next = next;
	} else {
		// This is the first one in the list, remove from the group!
		group->buffers_head = next;
	}

	// check if the group is empty while we still have the lock
	need_to_remove = (group->buffers_head == FREENEXT_END_OF_LIST);

	bg_unlock_buffers(group);

	// If the whole list is empty now, remove the block from the PQ bucket as well
	if (need_to_remove) {
		PQ_RemoveBlockGroup(group);
	}
}

/*
 * Estimate when the specific scan will reach the relevant block group.
 */
unsigned long ScanTimeToNextConsumption(const BlockGroupScanListElem *const bg_scan) {
	ScanHashEntry * s_data = bg_scan->scan_entry;
	ParallelBlockTableScanDesc pscan = s_data->data.pbscan;
	const BlockNumber blocks_behind = GROUP_TO_FIRST_BLOCK(bg_scan->blocks_behind);
	BlockNumber blocks_remaining;
	SharedScanStats stats;
	long res;

	// read stats from the struct
	stats = s_data->data.stats;

	if (pscan == NULL || blocks_behind >= s_data->data.pseq.nalloced) {
		/*
		 * Estimate time to next access time for:
		 *  - non-parallel sequential scans
		 *  - bitmap scans (parallel and non-parallel)
		 *  - parallel sequential scans where the block group has not been
		 *  	"allocated" to a worker yet, and we can use the over-all
		 *  	progress of the scan instead of needing to guess where its
		 *  	position within a chunk.
		 */
		// First: estimate distance (# blocks) to the block based on # of blocks scanned and position
		// 		of the block group in the scan
		// Then: distance/speed = time to next access (estimate)
		if (blocks_behind < stats.blocks_scanned) {
			// ### consider: if we've already been passed, then maybe this is "not requested" anymore?
			blocks_remaining = 0;
		} else {
			blocks_remaining = blocks_behind - stats.blocks_scanned;
		}

		res = (long) ((float) blocks_remaining / stats.est_speed);
	} else {
		/*
		 * Parallel sequential scan where the block is within one of the allocated chunks
		 *
		 * blocks_behind and nalloced are both relative to the start position, so
		 * using their difference and the chunk size we can figure out which chunk
		 * the block is in, and where it is in the chunk. (note: this might be off
		 * when chunk size decreases, which will cause blocks in the second half
		 * of a larger chunk to appear to be sooner than they should be. This seems
		 * not worth correcting)
		 */
		uint64 nalloced = s_data->data.pseq.nalloced;
		uint32 chunk_size = s_data->data.pseq.chunk_size;
		uint32 nworkers = s_data->data.pseq.nworkers;
		float worker_speed;

		Assert(chunk_size > 0);

		uint32 n_alloced_past = nalloced - blocks_behind;
		uint32 n_chunks_passed = n_alloced_past / chunk_size;
		uint32 chunk_start_behind = nalloced - chunk_size * (n_chunks_passed + 1);
		uint32 blks_since_chunk_start = blocks_behind - chunk_start_behind;

		/*
		 * Blocks from the start of the chunk: we don't know how far the worker
		 * is through the chunk, but it is between 0 and blks_since_start. On
		 * average it is half way, so use that as our guess.
		 */
		blocks_remaining = blks_since_chunk_start / 2;

		/*
		 * Scan speed: we are now considering just one worker, so its speed is lower.
		 * Assume each worker goes at the same rate and just take the average.
		 */
		worker_speed = stats.est_speed / (float)nworkers;

		/* Calculate the result */
		res = (long) ((float) blocks_remaining / worker_speed);
	}
	return res;
}

/*
 * Estimate the next access time of a block group based on the tracked metadata.
 */
unsigned long PageNextConsumption(BlockGroupData *const bgdata, bool *requestedPtr) {
	unsigned long min_next_access = AccessTimeNotRequested;
	slist_iter iter;

	Assert(bgdata != NULL);

	// Initially assume not requested
	*requestedPtr = false;

	// Lock the scan list before we start
	bg_lock_scans(bgdata, LW_SHARED);

	// not requested if there are no scans
	if (slist_is_empty(&bgdata->scans_list)) {
		bg_unlock_scans(bgdata);
		return AccessTimeNotRequested;
	}

	// loop over the scans and check the next access time estimate from that scan
	slist_foreach(iter, &bgdata->scans_list) {
		BlockGroupScanListElem * it = slist_container(BlockGroupScanListElem, slist, iter.cur);

		const unsigned long time_to_next_access = ScanTimeToNextConsumption(it);

		if (time_to_next_access != AccessTimeNotRequested
				&& time_to_next_access < min_next_access) {
			min_next_access = time_to_next_access;
			*requestedPtr = true;
		}
	}

	bg_unlock_scans(bgdata);

	// return the soonest next access time if applicable
	if (false == *requestedPtr) {
		return AccessTimeNotRequested;
	} else {
		Assert(min_next_access != AccessTimeNotRequested);
		return get_time_ns() + min_next_access;
	}
}

/* Delete a specific scan from the list of the given block group */
bool block_group_delete_scan(ScanId id, BlockGroupData *groupData) {
	slist_mutable_iter iter;

	// Lock the list before we start
	bg_lock_scans(groupData, LW_EXCLUSIVE);

	// Search the list to remove the scan
	slist_foreach_modify(iter, &groupData->scans_list) {
		BlockGroupScanListElem * it = slist_container(BlockGroupScanListElem, slist, iter.cur);

		// If we find the scan: remove it and add to free list, and then we are done
		if (it->scan_id == id) {
			slist_delete_current(&iter);

			bg_unlock_scans(groupData);
			free_bg_scan_elem(it);
			return true;
		}
	}

	// Didn't find it
	bg_unlock_scans(groupData);
	return false;
}

/*
 * Remove a scan from a range of blocks which may wrap around if lo > hi.
 *
 * Parameters are block *group* numbers.
 */
void remove_seq_scan_from_range_circular(pbm_bg_iterator *bg_it, const ScanHashEntry *const scan_entry, const uint32 lo, const uint32 hi) {
	const ScanId id = scan_entry->id;
	const BlockNumber nblocks = scan_entry->data.nblocks;

	/* Nothing to remove if there were no blocks to begin with */
	if (0 == nblocks) return;

	/* Remove the ranges, handling wrap-around if applicable */
	if (lo <= hi) {
		remove_seq_scan_from_block_range(bg_it, id, lo, hi);
	} else {
		BlockNumber upper = BLOCK_GROUP(nblocks - 1) + 1;
		remove_seq_scan_from_block_range(bg_it, id, lo, upper);
		remove_seq_scan_from_block_range(bg_it, id, 0, hi);
	}
}

/*
 * When the given scan is done, remove it from the remaining range of blocks.
 *
 * This acquires SHARED lock on PbmBlocksLock, and locks the individual
 * block groups as required.
 */
void remove_seq_scan_from_block_range(pbm_bg_iterator * bg_it, const ScanId id, const uint32 lo, const uint32 hi) {
	uint32 bgnum;
	uint32 i;
	BlockGroupHashEntry * bs_entry;

	// Nothing to do with empty range
	if (lo == hi) {
		return;
	}

	/* Sanity checks */
	Assert(lo <= hi);

	/* Find starting hash entry */
	bgit_advance_to(bg_it, lo);
	bs_entry = bg_it->entry;

	// Loop over the linked hash map entries
	bgnum 	= lo;
	i 		= bgnum % BLOCK_GROUP_SEG_SIZE;
	for ( ; bs_entry != NULL && bgnum < hi; bs_entry = bgit_advance_one(bg_it)) {
		// Loop over block groups in the entry
		for ( ; i < BLOCK_GROUP_SEG_SIZE && bgnum < hi; ++i, ++bgnum) {
			BlockGroupData * block_group = &bs_entry->groups[i];
			bool deleted = block_group_delete_scan(id, block_group);
			if (deleted) {
				RefreshBlockGroup(block_group);
			}
		}

		// start at first block group of the next entry
		i = 0;
	}

	/*
	 * DEBUGGING: make sure we could iterate all the way through...
	 */
	if (bgnum < hi) {
		elog(WARNING, "didnt get to the end of the range! expected %u but only got to %u",
			 hi, bgnum);
	}
}

/*
 * Remove a bitmap scan from current position in `scan_state` up to the specified hi endpoint.
 *
 * Used for both reporting progress and cleaning up at the end of a scan.
 */
int remove_bitmap_scan_from_block_range(const ScanId id, struct PBM_LocalBitmapScanState *const scan_state,
										const BlockNumber bg_hi) {
	pbm_bg_iterator bg_it = scan_state->bg_it;
	const bgcnt_vec *const v = &scan_state->block_groups;
	int i;

	/* Remove scan from the relevant block groups */
	for (i = scan_state->vec_idx; bg_hi > v->items[i].block_group; ++i) {
		const BlockNumber blk = v->items[i].block_group;
		BlockGroupData * data;
		bool deleted;

		/* Don't go past the end of the list */
		if (i >= v->len) {
			break;
		}

		/* Advance segment iterator to the next needed segment if applicable */
		data = bgit_advance_to(&bg_it, blk);

		/* Delete scan from the group and refresh the group if applicable */
		deleted = block_group_delete_scan(id, data);
		if (deleted) {
			RefreshBlockGroup(data);
		}
	}

	return i;
}

/*
 * Refresh a block group in the PQ.
 *
 * This computes the next consumption time and moves the block group to the appropriate bucket,
 * removing it from the current one first.
 */
void RefreshBlockGroup(BlockGroupData *const data) {
	bool requested;
	bool has_buffers = (data->buffers_head >= 0);
	unsigned long t;

	// Check if this group should be in the PQ.
	// If so, move it to the appropriate bucket. If not, remove it from its bucket if applicable.
	if (has_buffers) {
		t = PageNextConsumption(data, &requested);
		PQ_RefreshBlockGroup(data, t, requested);
	} else {
		PQ_RemoveBlockGroup(data);
	}
}

/*
 * If enough time has passed, shift the PQ buckets to reflect the passage of time.
 *
 * This should generally be called just before inserting or refreshing a block
 * group. We don't want to put a block group in the wrong bucket just because
 * the buckets are out of date.
 */
void PQ_RefreshRequestedBuckets(void) {
	unsigned long ts = get_timeslice();
	unsigned long last_shifted_ts = pbm->BlockQueue->last_shifted_time_slice;
	bool up_to_date = (last_shifted_ts + 1 > ts);

#if defined(TRACE_PBM) && defined(TRACE_PBM_PQ_REFRESH)
	elog(INFO, "PBM refresh buckets: t=%ld, last=%ld, up_to_date=%s",
		 ts, last_shifted_ts, up_to_date?"true":"false");
#endif // TRACE_PBM_PQ_REFRESH

	// Nothing to do if already up to date
	if (up_to_date) return;

	// Shift the PQ buckets as many times as necessary to catch up
	LOCK_GUARD_V2(PbmPqBucketsLock, LW_EXCLUSIVE) {
		while (PQ_ShiftBucketsWithLock(ts)) ;
	}
}



#if PBM_EVICT_MODE == PBM_EVICT_MODE_SINGLE
BufferDesc* PBM_EvictPage(uint32 * buf_state) {
	return PQ_Evict(pbm->BlockQueue, buf_state);
}
#endif // PBM_EVICT_MODE


// ### clean up the debugging code below

#ifdef SANITY_PBM_BUFFERS
void sanity_check_verify_block_group_buffers(const BufferDesc * const buf) {
	const RelFileNode rnode = buf->tag.rnode;
	const ForkNumber  fork = buf->tag.forkNum;
	const BlockNumber bgroup = BLOCK_GROUP(buf->tag.blockNum);

	int num_traversed = 0;

	// get first buffer in the list
	const BufferDesc * it = buf;

	Assert(it->pbm_bgroup_prev > FREENEXT_NOT_IN_LIST && it->pbm_bgroup_next > FREENEXT_NOT_IN_LIST);

	while (it->pbm_bgroup_prev != FREENEXT_END_OF_LIST) {
		it = GetBufferDescriptor(it->pbm_bgroup_prev);
		++num_traversed;
		if (num_traversed % 100 == 0) {
			elog(WARNING, "sanity_check traversed 100 blocks!");

			it = buf;
			for (int i = 0; i < 10; ++i) {
				const BufferTag tag = it->tag;
				elog(WARNING, " tbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u (group=%u) prev=%d next=%d",
					 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum, BLOCK_GROUP(tag.blockNum),
					 it->pbm_bgroup_prev, it->pbm_bgroup_next
				);
				it = GetBufferDescriptor(it->pbm_bgroup_prev);
			}

			elog(ERROR, "sanity_check traversed 100 blocks!");
			return;
		}
	}

	// make sure everything in the list has the same group
	while (it != NULL) {
		if (it->tag.rnode.spcNode != rnode.spcNode
			|| it->tag.rnode.dbNode != rnode.dbNode
			|| it->tag.rnode.relNode != rnode.relNode
			|| it->tag.forkNum != fork
			|| BLOCK_GROUP(it->tag.blockNum) != bgroup)
		{
			const BufferTag tag = it->tag;

			list_all_buffers();

			/* Note: this check is likely to fail with parallel workloads
			 * because the buffers can get moved around while we're checking
			 * everything. */
			elog(ERROR, "BLOCK GROUP has buffer from the wrong group!  buf_id=%d"
						"\n\texpected: \ttbl={spc=%u, db=%u, rel=%u, fork=%d} block_group=%u"
						"\n\tgot:      \ttbl={spc=%u, db=%u, rel=%u, fork=%d} block#=%u (group=%u)",
				 it->buf_id,
				 rnode.spcNode, rnode.dbNode, rnode.relNode, fork, bgroup,
				 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum, BLOCK_GROUP(tag.blockNum)
			);
		}
		if (FREENEXT_END_OF_LIST == it->pbm_bgroup_next) it = NULL;
		else it = GetBufferDescriptor(it->pbm_bgroup_next);
	}
}

void list_all_buffers(void) {
	bool found;
	BlockGroupData * data;
	Buffer bid;

	for (int i = 0; i < NBuffers; ++i) {
		BufferDesc *buf = GetBufferDescriptor(i);
		BufferTag tag = buf->tag;
		elog(WARNING, "BLOCK %d: \tspc=%u, db=%u, rel=%u, fork=%d, block=%u (group=%u) \tprev=%d  next=%d",
			 i, tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum,
			 BLOCK_GROUP(tag.blockNum), buf->pbm_bgroup_prev, buf->pbm_bgroup_next
		);

		data = search_block_group(buf, &found);

		if (!found || data == NULL) {
			elog(WARNING, "\tGROUP NOT FOUND!  prev=%d  next=%d", buf->pbm_bgroup_prev, buf->pbm_bgroup_next);
			continue;
		}

		for (bid = data->buffers_head; bid >= 0 && bid <= NBuffers; bid = GetBufferDescriptor(bid)->pbm_bgroup_next) {
			BufferDesc *buf2 = GetBufferDescriptor(bid);
			tag = buf2->tag;
			elog(WARNING, "\tbid=%d:  \tspc=%u, db=%u, rel=%u, fork=%d, block=%u (group=%u) \tPREV=%d, NEXT=%d",
				 bid,
				 tag.rnode.spcNode, tag.rnode.dbNode, tag.rnode.relNode, tag.forkNum, tag.blockNum,
				 BLOCK_GROUP(tag.blockNum), buf2->pbm_bgroup_prev, buf2->pbm_bgroup_next
			);
		}
	}
}
#endif // SANITY_PBM_BUFFERS

void debug_buffer_access(BufferDesc* buf, char* msg) {
	bool found, requested;
	char* msg2;
	BlockNumber blockNum = buf->tag.blockNum;
	BlockNumber blockGroup = BLOCK_GROUP(blockNum);
	TableData tbl = (TableData){
			.rnode = buf->tag.rnode,
			.forkNum = buf->tag.forkNum,
	};
	unsigned long next_access_time = AccessTimeNotRequested;
	unsigned long now = get_time_ns();

	BlockGroupData *const block_scans = search_block_group(buf, &found);
	if (true == found) {
		next_access_time = PageNextConsumption(block_scans, &requested);
	}

	if (false == found) msg2 = "NOT TRACKED";
	else if (false == requested) msg2 = "NOT REQUESTED";
	else msg2 = "~";

	elog(INFO, "PBM %s (%s): tbl={spc=%u, db=%u, rel=%u} block=%u group=%u --- now=%lu, next_access=%lu",
		 msg,
		 msg2,
		 tbl.rnode.spcNode,
		 tbl.rnode.dbNode,
		 tbl.rnode.relNode,
		 blockNum,
		 blockGroup,
		 now,
		 next_access_time
	);
}

// Print debugging information for a single entry in the scans hash map.
static
void debug_append_scan_data(StringInfoData* str, ScanHashEntry* entry) {
	SharedScanStats stats = entry->data.stats;
	appendStringInfo(str, "{id=%lu, start=%u, nblocks=%u, speed=%f}",
					 entry->id,
					 entry->data.startBlock,
					 entry->data.nblocks,
					 stats.est_speed
	);
}

// Print the whole scans map for debugging.
void debug_log_scan_map(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmScansLock, LW_SHARED) {
		HASH_SEQ_STATUS status;
		ScanHashEntry * entry;

		hash_seq_init(&status, pbm->ScanMap);

		for (;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			appendStringInfoString(&str, "\n\t");
			debug_append_scan_data(&str, entry);
		}
	}

	ereport(INFO, (errmsg_internal("PBM scan map:"), errdetail_internal("%s", str.data)));
	pfree(str.data);
}

// Append debugging information for one block group
static
void debug_append_bg_data(StringInfoData *str, BlockGroupData *data, BlockNumber bgroup) {
	BlockNumber seg = BLOCK_GROUP_SEGMENT(bgroup);
	slist_iter it;

	appendStringInfo(str, "\n\t\tseg=%u, bg=%3u, addr=%p scans=",
		seg, bgroup, data
	);

	bg_lock_scans(data, LW_SHARED);

	slist_foreach(it, &data->scans_list) {
		BlockGroupScanListElem * elem = slist_container(BlockGroupScanListElem, slist, it.cur);

		appendStringInfo(str, " {id=%3lu, behind=%5u}",
			elem->scan_id, elem->blocks_behind
		);
	}

	bg_unlock_scans(data);
}

// Print debugging information for a relation in the block group map
static
void debug_append_bgseg_data(StringInfoData* str, BlockGroupHashEntry* entry) {
	appendStringInfo(str, "\n\ttbl={spc=%u, db=%u, rel=%u, fork=%u}",
		entry->key.rnode.spcNode, entry->key.rnode.dbNode, entry->key.rnode.relNode, entry->key.forkNum
	);

	// Print something for each segment
	for ( ; NULL != entry; entry = entry->seg_next) {
		BlockGroupData * data = &entry->groups[0];
		debug_append_bg_data(str, data, entry->key.seg * BLOCK_GROUP_SEG_SIZE);
		debug_append_bg_data(str, data + 1, entry->key.seg * BLOCK_GROUP_SEG_SIZE + 1);
		debug_append_bg_data(str, data + 16, entry->key.seg * BLOCK_GROUP_SEG_SIZE + 16);
		debug_append_bg_data(str, data + 128, entry->key.seg * BLOCK_GROUP_SEG_SIZE + 128);
	}
}

// Print the whole block group map for debugging
void debug_log_blockgroup_map(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		HASH_SEQ_STATUS status;
		BlockGroupHashEntry * entry;

		hash_seq_init(&status, pbm->BlockGroupMap);

		for (;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			// do each table in order
			if (entry->key.seg != 0) continue;

			debug_append_bgseg_data(&str, entry);
		}
	}

	ereport(INFO, (errmsg_internal("PBM block group map:"), errdetail_internal("%s", str.data)));
	pfree(str.data);
}

void debug_log_find_blockgroup_buffers(void) {
	StringInfoData str;
	initStringInfo(&str);

	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		HASH_SEQ_STATUS status;
		BlockGroupHashEntry * entry;

		hash_seq_init(&status, pbm->BlockGroupMap);

		// each hash entry
		for (;;) {
			entry = hash_seq_search(&status);
			if (NULL == entry) break;

			// each group in the entry
			for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
				BlockGroupData * data = &entry->groups[i];
				Buffer b;

				// print out block group if it has buffers
				if (data->buffers_head != FREENEXT_END_OF_LIST) {
					appendStringInfo(&str, "\n\ttbl={spc=%u, db=%u, rel=%u, fork=%d} "
										   "blk=%d :  "
						, entry->key.rnode.spcNode, entry->key.rnode.dbNode, entry->key.rnode.relNode, entry->key.forkNum
						, entry->key.seg * BLOCK_GROUP_SEG_SIZE + i
					);

					// append list of buffers for the block group
					for(b = data->buffers_head; b >= 0; ) {
						BufferDesc * buf = GetBufferDescriptor(b);
						appendStringInfo(&str, " %d", b);

						b = buf->pbm_bgroup_next;
					}
					appendStringInfo(&str, " %d", b);
				}
			}
		}
	}

	ereport(INFO, (errmsg_internal("PBM block groups:"), errdetail_internal("%s", str.data)));

	pfree(str.data);
}

/* Assert the given scan has been completely removed from everything */
void assert_scan_completely_unregistered(ScanHashEntry * scan) {
#ifdef SANITY_PBM_SCAN_FULLY_UNREGISTERED
	bool found;
	int bgnum = 0;
	BlockGroupHashEntry * bgseg_it;
	BlockGroupHashKey bgkey = {
		.rnode = scan->data.tbl.rnode,
		.forkNum = scan->data.tbl.forkNum,
		.seg = 0,
	};

	/* First segment */
	LOCK_GUARD_V2(PbmBlocksLock, LW_SHARED) {
		bgseg_it = hash_search(pbm->BlockGroupMap, &bgkey, HASH_FIND, &found);
	}
	Assert(found);

	for ( ; bgseg_it != NULL; bgseg_it = bgseg_it->seg_next) {
		for (int i = 0; i < BLOCK_GROUP_SEG_SIZE; ++i) {
			BlockGroupData * bg = &bgseg_it->groups[i];
			slist_iter it;

			bg_lock_scans(bg, LW_SHARED);

			slist_foreach(it, &bg->scans_list) {
				BlockGroupScanListElem * elem = slist_container(BlockGroupScanListElem, slist, it.cur);

				if (elem->scan_id == scan->id) {
					elog(ERROR, "Scan %lu was not fully unregistered! still registered for block group %d "
								"tbl={spc=%u, db=%u, rel=%u, fork=%u}"
						, scan->id, bgnum
						, bgkey.rnode.spcNode, bgkey.rnode.dbNode, bgkey.rnode.relNode, bgkey.forkNum
					);
				}
			}

			bg_unlock_scans(bg);
			++bgnum;
		}
	}
#endif /* SANITY_PBM_SCAN_FULLY_UNREGISTERED */
}
