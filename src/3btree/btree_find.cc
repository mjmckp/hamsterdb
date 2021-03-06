/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * btree searching
 */

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_node_proxy.h"
#include "3page_manager/page_manager.h"
#include "4cursor/cursor_local.h"
#include "4db/db.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class BtreeFindAction
{
  public:
    BtreeFindAction(BtreeIndex *btree, Context *context, LocalCursor *cursor,
                    ups_key_t *key, ByteArray *key_arena,
                    ups_record_t *record, ByteArray *record_arena,
                    uint32_t flags)
      : m_btree(btree), m_context(context), m_cursor(0), m_key(key),
        m_record(record), m_flags(flags), m_key_arena(key_arena),
        m_record_arena(record_arena) {
      if (cursor && cursor->get_btree_cursor()->get_parent())
        m_cursor = cursor->get_btree_cursor();
    }

    ups_status_t run() {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->lenv();
      Page *page = 0;
      int slot = -1;
      BtreeNodeProxy *node = 0;

      BtreeStatistics *stats = m_btree->get_statistics();
      BtreeStatistics::FindHints hints = stats->get_find_hints(m_flags);

      if (hints.try_fast_track) {
        /*
         * see if we get a sure hit within this btree leaf; if not, revert to
         * regular scan
         *
         * As this is a speed-improvement hint re-using recent material, the
         * page should still sit in the cache, or we're using old info, which
         * should be discarded.
         */
        page = env->page_manager()->fetch(m_context, hints.leaf_page_addr,
                                            PageManager::kOnlyFromCache
                                            | PageManager::kReadOnly);
        if (page) {
          node = m_btree->get_node_from_page(page);
          ups_assert(node->is_leaf());

          uint32_t approx_match;
          slot = find(m_context, page, m_key, m_flags, &approx_match);

          /*
           * if we didn't hit a match OR a match at either edge, FAIL.
           * A match at one of the edges is very risky, as this can also
           * signal a match far away from the current node, so we need
           * the full tree traversal then.
           */
          if (approx_match || slot <= 0 || slot >= (int)node->get_count() - 1)
            slot = -1;

          /* fall through */
        }
      }

      uint32_t approx_match = 0;

      if (slot == -1) {
        /* load the root page */
        page = env->page_manager()->fetch(m_context,
                        m_btree->root_address(), PageManager::kReadOnly);

        /* now traverse the root to the leaf nodes till we find a leaf */
        node = m_btree->get_node_from_page(page);
        while (!node->is_leaf()) {
          page = m_btree->find_lower_bound(m_context, page, m_key,
                                PageManager::kReadOnly, 0);
          if (!page) {
            stats->find_failed();
            return (UPS_KEY_NOT_FOUND);
          }

          node = m_btree->get_node_from_page(page);
        }

        /* check the leaf page for the key (shortcut w/o approx. matching) */
        if (m_flags == 0) {
          slot = node->find(m_context, m_key);
          if (slot == -1) {
            stats->find_failed();
            return (UPS_KEY_NOT_FOUND);
          }
          goto return_result;
        }

        /* check the leaf page for the key (long path w/ approx. matching),
         * then fall through */
        slot = find(m_context, page, m_key, m_flags, &approx_match);
      }

      if (slot == -1) {
        // find the left sibling
        if (node->get_left() > 0) {
          page = env->page_manager()->fetch(m_context, node->get_left(),
                          PageManager::kReadOnly);
          node = m_btree->get_node_from_page(page);
          slot = node->get_count() - 1;
          approx_match = BtreeKey::kLower;
        }
      }

      else if (slot >= (int)node->get_count()) {
        // find the right sibling
        if (node->get_right() > 0) {
          page = env->page_manager()->fetch(m_context, node->get_right(),
                          PageManager::kReadOnly);
          node = m_btree->get_node_from_page(page);
          slot = 0;
          approx_match = BtreeKey::kGreater;
        }
        else
          slot = -1;
      }

      if (slot < 0) {
        stats->find_failed();
        return (UPS_KEY_NOT_FOUND);
      }

      ups_assert(node->is_leaf());

return_result:
      /* set the cursor-position to this key */
      if (m_cursor) {
        m_cursor->couple_to_page(page, slot, 0);
      }

      /* approx. match: patch the key flags */
      if (approx_match) {
        ups_key_set_intflags(m_key, approx_match);
      }

      /* no need to load the key if we have an exact match, or if KEY_DONT_LOAD
       * is set: */
      if (m_key && approx_match && !(m_flags & LocalCursor::kSyncDontLoadKey)) {
        node->get_key(m_context, slot, m_key_arena, m_key);
      }

      if (m_record) {
        node->get_record(m_context, slot, m_record_arena, m_record, m_flags);
      }

      return (0);
    }

  private:
    // Searches a leaf node for a key.
    //
    // !!!
    // only works with leaf nodes!!
    //
    // Returns the index of the key, or -1 if the key was not found, or
    // another negative status code value when an unexpected error occurred.
    int find(Context *context, Page *page, ups_key_t *key, uint32_t flags,
                    uint32_t *approx_match) {
      *approx_match = 0;

      /* ensure the approx flag is NOT set by anyone yet */
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      if (node->get_count() == 0)
        return (-1);

      int cmp;
      int slot = node->find_lower_bound(context, key, 0, &cmp);

      /* successfull match */
      if (cmp == 0 && (flags == 0 || flags & UPS_FIND_EQ_MATCH))
        return (slot);

      /* approx. matching: smaller key is required */
      if (flags & UPS_FIND_LT_MATCH) {
        if (cmp == 0 && (flags & UPS_FIND_GT_MATCH)) {
          *approx_match = BtreeKey::kLower;
          return (slot + 1);
        }

        if (slot < 0 && (flags & UPS_FIND_GT_MATCH)) {
          *approx_match = BtreeKey::kGreater;
          return (0);
        }
        *approx_match = BtreeKey::kLower;
        if (cmp <= 0)
          return (slot - 1);
        return (slot);
      }

      /* approx. matching: greater key is required */
      if (flags & UPS_FIND_GT_MATCH) {
        *approx_match = BtreeKey::kGreater;
        return (slot + 1);
      }

      return (cmp ? -1 : slot);
    }

    // the current btree
    BtreeIndex *m_btree;

    // The caller's Context
    Context *m_context;

    // the current cursor
    BtreeCursor *m_cursor;

    // the key that is retrieved
    ups_key_t *m_key;

    // the record that is retrieved
    ups_record_t *m_record;

    // flags of ups_db_find()
    uint32_t m_flags;

    // allocator for the key data
    ByteArray *m_key_arena;

    // allocator for the record data
    ByteArray *m_record_arena;
};

ups_status_t
BtreeIndex::find(Context *context, LocalCursor *cursor, ups_key_t *key,
                ByteArray *key_arena, ups_record_t *record,
                ByteArray *record_arena, uint32_t flags)
{
  BtreeFindAction bfa(this, context, cursor, key, key_arena, record,
                  record_arena, flags);
  return (bfa.run());
}

} // namespace upscaledb

