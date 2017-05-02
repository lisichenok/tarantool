/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "vy_write_iterator.h"
#include "vy_mem.h"
#include "vy_run.h"
#include "vinyl.h"

#define HEAP_FORWARD_DECLARATION
#include "salad/heap.h"

static bool
heap_less(heap_t *heap, struct heap_node *n1, struct heap_node *n2);

#define HEAP_NAME src_heap
#define HEAP_LESS heap_less
#include "salad/heap.h"

/**
 * Merge source of write iterator. Represents a mem or a run.
 */
struct vy_write_src {
	/* Link in vy_write_iterator::src_list */
	struct rlist in_src_list;
	/* Node in vy_write_iterator::src_heap */
	struct heap_node heap_node;
	/* Current tuple in the source (lowest and with maximal lsn) */
	const struct tuple *tuple;
	/**
	 * Is the tuple (@sa tuple) refable or not.
	 * Tuples from mems are reafble, from runs - not
	 */
	bool tuple_refable;
	/** Source iterator */
	union {
		struct vy_run_stream run_stream;
		struct vy_mem_stream mem_stream;
		struct vy_stmt_stream stream;
	};
};

/**
 * Write iterator itself.
 */
struct vy_write_iterator {
	/* List of all sources of the iterator */
	struct rlist src_list;
	/* Heap with sources with the lowest source in head */
	heap_t src_heap;
	/**
	 * Tuple that is was returned in last vy_write_iterator_next call,
	 * or the tuple to be returned during vy_write_iterator_next execution
	 */
	const struct tuple *tuple;
	/**
	 * Is the tuple (@sa tuple) refable or not.
	 * Tuples from mems are reafble, from runs - not
	 */
	bool tuple_refable;
	/**
	 * Block heap node that represnents a boundary in merged steam
	 * between current tuple and the next tuple. Is inserted into
	 * src_heap during vy_write_iterator_next_key call, and when it
	 * comes from heap that means that there are only greater keys
	 * in heap.
	 */
	struct heap_node *key_end_node;
	/** Index key definition used for storing statements on disk. */
	const struct key_def *key_def;
	/** Format to allocate new REPLACE and DELETE tuples from vy_run */
	struct tuple_format *format;
	/** Same as surrogate_format, but for UPSERT tuples. */
	struct tuple_format *upsert_format;
	/** Index column mask. */
	uint64_t column_mask;
	/* The minimal VLSN among all active transactions */
	int64_t oldest_vlsn;
	/* There are is no level older than the one we're writing to. */
	bool is_last_level;
	/** Set if this iterator is for a primary index. */
	bool is_primary;
};

/**
 * Comparator of the heap. Generally compares two sources and finds out
 * whether one source is less than another.
 */
static bool
heap_less(heap_t *heap, struct heap_node *node1, struct heap_node *node2)
{
	struct vy_write_iterator *strm =
		container_of(heap, struct vy_write_iterator, src_heap);

	/**
	 * There might be key_end_node in one of the comparables.
	 * This specail value means that strm->tuple must be compared.
	 */
	assert(node1 != strm->key_end_node || node2 != strm->key_end_node);
	const struct tuple *tuple1 = strm->tuple, *tuple2 = strm->tuple;
	if (node1 != strm->key_end_node) {
		struct vy_write_src *src =
			container_of(node1, struct vy_write_src, heap_node);
		tuple1 = src->tuple;
	}
	if (node2 != strm->key_end_node) {
		struct vy_write_src *src =
			container_of(node2, struct vy_write_src, heap_node);
		tuple2 = src->tuple;
	}

	int cmp = tuple_compare(tuple1, tuple2, strm->key_def);
	if (cmp != 0)
		return cmp < 0;

	/**
	 * key_end_node must be considered as greater any of the tuples
	 * with equal key.
	 */
	if (node1 == strm->key_end_node)
		return false;
	else if(node2 == strm->key_end_node)
		return true;

	/** Keys are equal, order by lsn it descent order */
	if (vy_stmt_lsn(tuple1) > vy_stmt_lsn(tuple2))
		return true;
	else if (vy_stmt_lsn(tuple1) < vy_stmt_lsn(tuple2))
		return false;
	assert(vy_stmt_lsn(tuple1) == vy_stmt_lsn(tuple2));

	/** lsns are equal, prioritize terminal (non-upsert) statements */
	return (vy_stmt_type(tuple1) == IPROTO_UPSERT ? 1 : 0) <
	       (vy_stmt_type(tuple2) == IPROTO_UPSERT ? 1 : 0);

}

/**
 * Allocate a source and put it to the list.
 * The underlying stream (src->stream) must be opened immediately.
 * @param strm - the write iterator.
 * @param tuple_refable - true for runs and false for mems.
 * @return the source or NULL on memory error.
 */
static struct vy_write_src *
vy_write_iterator_new_src(struct vy_write_iterator *strm, bool tuple_refable)
{
	struct vy_write_src *res = (struct vy_write_src *) malloc(sizeof(*res));
	if (res == NULL) {
		diag_set(OutOfMemory, sizeof(*res),
			 "malloc", "write stream src");
		return NULL;
	}
	res->tuple_refable = tuple_refable;
	rlist_add(&strm->src_list, &res->in_src_list);
	return res;
}


/**
 * Close the stream of the source, remove from list and delete.
 */
static void
vy_write_iterator_delete_src(struct vy_write_iterator *strm,
			     struct vy_write_src *src)
{
	(void)strm;
	src->stream.iface->close(&src->stream);
	rlist_del(&src->in_src_list);
	free(src);
}

/**
 * Put the source to the heap. Source's stream must be opened.
 * @return 0 - success, not 0 - error.
 */
static NODISCARD int
vy_write_iterator_add_src(struct vy_write_iterator *strm,
			  struct vy_write_src *src)
{
	int rc = src->stream.iface->next(&src->stream, &src->tuple);
	if (rc != 0 || src->tuple == NULL) {
		vy_write_iterator_delete_src(strm, src);
		return rc;
	}
	rc = src_heap_insert(&strm->src_heap, &src->heap_node);
	if (rc != 0) {
		diag_set(OutOfMemory, sizeof(void *),
			 "malloc", "write stream heap");
		vy_write_iterator_delete_src(strm, src);
		return rc;
	}
	return 0;
}

/**
 * Remove the source from the heap, destroy and free it.
 */
static void
vy_write_iterator_remove_src(struct vy_write_iterator *strm,
			   struct vy_write_src *src)
{
	src_heap_delete(&strm->src_heap, &src->heap_node);
	vy_write_iterator_delete_src(strm, src);
}

/**
 * Open an empty write iterator. To add sources to the iterator
 * use vy_write_iterator_add_* functions.
 * @return the iterator or NULL on error (diag is set).
 */
struct vy_write_iterator *
vy_write_iterator_new(const struct key_def *key_def, struct tuple_format *format,
		    struct tuple_format *upsert_format, bool is_primary,
		    uint64_t column_mask, bool is_last_level,
		    int64_t oldest_vlsn)
{
	struct vy_write_iterator *strm =
		(struct vy_write_iterator *) malloc(sizeof(*strm));
	if (strm == NULL) {
		diag_set(OutOfMemory, sizeof(*strm), "malloc", "write stream");
		return NULL;
	}
	src_heap_create(&strm->src_heap);
	rlist_create(&strm->src_list);
	strm->key_def = key_def;
	strm->format = format;
	strm->upsert_format = upsert_format;
	strm->is_primary = is_primary;
	strm->column_mask = column_mask;
	strm->oldest_vlsn = oldest_vlsn;
	strm->is_last_level = is_last_level;
	strm->tuple = NULL;
	strm->tuple_refable = false;
	return strm;
}

/**
 * Set strm->tuple as a tuple to be output as a result of .._next call.
 * Ref the new tuple if necessary, unref older value if needed.
 * @param strm - the write iterator.
 * @param tuple - the tuple to be saved.
 * @param tuple_refable - is the tuple must of must not be referenced.
 */
static void
vy_write_iterator_set_tuple(struct vy_write_iterator *strm,
			    const struct tuple *tuple, bool tuple_refable)
{
	if (strm->tuple != NULL && tuple != NULL)
		assert(tuple_compare(strm->tuple, tuple, strm->key_def) < 0 ||
			vy_stmt_lsn(strm->tuple) >= vy_stmt_lsn(tuple));

	if (strm->tuple != NULL && strm->tuple_refable)
		tuple_unref((struct tuple *)strm->tuple);

	strm->tuple = tuple;
	strm->tuple_refable = tuple_refable;

	if (strm->tuple != NULL && strm->tuple_refable)
		tuple_ref((struct tuple *)strm->tuple);
}

/**
 * Delete the iterator.
 */
void
vy_write_iterator_delete(struct vy_write_iterator *strm)
{
	vy_write_iterator_set_tuple(strm, NULL, false);
	struct vy_write_src *src, *tmp;
	rlist_foreach_entry_safe(src, &strm->src_list, in_src_list, tmp)
		vy_write_iterator_delete_src(strm, src);
	free(strm);
}

/**
 * Add a mem as a source of iterator.
 * @return 0 on success or not 0 on error (diag is set).
 */
NODISCARD int
vy_write_iterator_add_mem(struct vy_write_iterator *strm, struct vy_mem *mem)
{
	struct vy_write_src *src = vy_write_iterator_new_src(strm, false);
	if (src == NULL)
		return -1;
	vy_mem_stream_open(&src->mem_stream, mem);
	return vy_write_iterator_add_src(strm, src);
}

/**
 * Add a run as a source of iterator.
 * @return 0 on success or not 0 on error (diag is set).
 */
NODISCARD int
vy_write_iterator_add_run(struct vy_write_iterator *strm, struct vy_run *run,
			pthread_key_t *zdctx_key)
{
	struct vy_write_src *src = vy_write_iterator_new_src(strm, true);
	if (src == NULL)
		return -1;
	vy_run_stream_open(&src->run_stream, run, strm->key_def, strm->format,
			   strm->upsert_format, zdctx_key, strm->is_primary);
	return vy_write_iterator_add_src(strm, src);
}

/**
 * Don't modify indexes whose fields were not changed by update.
 * If there is at least one bit in the column mask
 * (@sa update_read_ops in tuple_update.cc) set that corresponds
 * to one of the columns from index_def->parts, then the update
 * operation changes at least one indexed field and the
 * optimization is inapplicable. Otherwise, we can skip the
 * update.
 * @param index_column_mask Mask of index we try to update.
 * @param stmt_column_mask  Maks of the update operations.
 */
static bool
vy_can_skip_update(uint64_t index_column_mask, uint64_t stmt_column_mask)
{
	/*
	 * Update of the primary index can't be skipped, since it
	 * stores not indexes tuple fields besides indexed.
	 */
	return (index_column_mask & stmt_column_mask) == 0;
}

/**
 * Go to next tuple in terms of sorted (merges) input steams.
 * @return 0 on success or not 0 on error (diag is set).
 */
static NODISCARD int
vy_write_iterator_step(struct vy_write_iterator *strm)
{
	struct heap_node *node = src_heap_top(&strm->src_heap);
	assert(node != NULL);
	struct vy_write_src *src = container_of(node, struct vy_write_src,
						heap_node);
	int rc = src->stream.iface->next(&src->stream, &src->tuple);
	if (rc != 0)
		return rc;
	if (src->tuple != NULL)
		src_heap_update(&strm->src_heap, node);
	else
		vy_write_iterator_remove_src(strm, src);
	return 0;
}

/**
 * Squash in the single statement all rest statements of current key
 * starting from the current statement.
 */
static NODISCARD int
vy_write_iterator_next_key(struct vy_write_iterator *strm)
{
	assert(strm->tuple != NULL);
	struct heap_node key_end_node;
	strm->key_end_node = &key_end_node;
	int rc = src_heap_insert(&strm->src_heap, strm->key_end_node);
	if (rc) {
		diag_set(OutOfMemory, sizeof(void *),
			 "malloc", "write stream heap");
		strm->key_end_node = NULL;
		return rc;
	}
	while (true) {
		struct heap_node *node = src_heap_top(&strm->src_heap);
		assert(node != NULL);
		struct vy_write_src *src = node == strm->key_end_node ? NULL :
			container_of(node, struct vy_write_src, heap_node);

		if (vy_stmt_type(strm->tuple) == IPROTO_UPSERT &&
		    (node != strm->key_end_node || strm->is_last_level)) {
			struct tuple *applied;
			applied = vy_apply_upsert(strm->tuple,
						  src ? src->tuple : NULL,
						  strm->key_def,
						  strm->format,
						  strm->upsert_format,
						  strm->is_primary,
						  false, NULL);
			if (applied == NULL) {
				rc = -1;
				break;
			}
			vy_write_iterator_set_tuple(strm, applied, true);
		}

		if (node == strm->key_end_node)
			break;

		rc = vy_write_iterator_step(strm);
		if (rc != 0)
			break;
	}
	src_heap_delete(&strm->src_heap, strm->key_end_node);
	strm->key_end_node = NULL;
	return rc;
}

/**
 * Get the next statement to write.
 * The user of the write iterator simply expects a stream
 * of statements to write to the output.
 * The tuple *ret is guaranteed to be valid until the next
 *  vy_write_iterator_next call.
 *
 * @return 0 on success or not 0 on error (diag is set).
 */
NODISCARD int
vy_write_iterator_next(struct vy_write_iterator *strm, const struct tuple **ret)
{
	/*
	 * Nullify the result stmt. If the next stmt is not
	 * found, this would be a marker of the end of the stream.
	 */
	*ret = NULL;

	while (true) {
		struct heap_node *node = src_heap_top(&strm->src_heap);
		if (node == NULL)
			return 0; /* no more data */
		struct vy_write_src *src =
			container_of(node, struct vy_write_src, heap_node);
		vy_write_iterator_set_tuple(strm, src->tuple,
					    src->tuple_refable);

		int rc = vy_write_iterator_step(strm);
		if (rc != 0)
			return -1;

		if (vy_stmt_lsn(strm->tuple) > strm->oldest_vlsn)
			break; /* Save the current stmt as the result. */

		if (vy_stmt_type(strm->tuple) == IPROTO_REPLACE ||
		    vy_stmt_type(strm->tuple) == IPROTO_DELETE) {
			/*
			 * If the tuple has extra size - it has
			 * column mask of an update operation.
			 * The tuples from secondary indexes
			 * which don't modify its keys can be
			 * skipped during dump,
			 * @sa vy_can_skip_update().
			 */
			if (!strm->is_primary &&
			    vy_can_skip_update(strm->column_mask,
					       vy_stmt_column_mask(strm->tuple)))
				continue;
		}

		/* Squash upserts or go to the next key */
		rc = vy_write_iterator_next_key(strm);
		if (rc != 0)
			return -1;

		if (vy_stmt_type(strm->tuple) == IPROTO_DELETE &&
		    strm->is_last_level)
			continue; /* Skip unnecessary DELETE */
		break;
	}
	*ret = strm->tuple;
	return 0;
}
