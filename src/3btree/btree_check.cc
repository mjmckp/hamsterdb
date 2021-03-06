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
 * btree verification
 */

#include "0root/root.h"

#include <set>
#include <string.h>
#include <stdio.h>
#if UPS_DEBUG
#  include <sstream>
#  include <fstream>
#endif

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_test.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class BtreeCheckAction
{
  public:
    // Constructor
    BtreeCheckAction(BtreeIndex *btree, Context *context, uint32_t flags)
      : m_btree(btree), m_context(context), m_flags(flags) {
    }

    // This is the main method; it starts the verification.
    void run() {
      Page *page, *parent = 0;
      uint32_t level = 0;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->lenv();

      // get the root page of the tree
      page = env->page_manager()->fetch(m_context, m_btree->root_address(),
                                    PageManager::kReadOnly);

#if UPS_DEBUG
      if (m_flags & UPS_PRINT_GRAPH) {
        m_graph << "digraph g {" << std::endl
                << "  graph [" << std::endl
                << "    rankdir = \"TD\"" << std::endl
                << "  ];" << std::endl
                << "  node [" << std::endl
                << "    fontsize = \"8\"" << std::endl
                << "    shape = \"ellipse\"" << std::endl
                << "  ];" << std::endl
                << "  edge [" << std::endl
                << "  ];" << std::endl;
      }
#endif

      // for each level...
      while (page) {
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        uint64_t ptr_down = node->get_ptr_down();

        // verify the page and all its siblings
        verify_level(parent, page, level);
        parent = page;

        // follow the pointer to the smallest child
        if (ptr_down)
          page = env->page_manager()->fetch(m_context, ptr_down,
                                PageManager::kReadOnly);
        else
          page = 0;

        ++level;
      }

#if UPS_DEBUG
      if (m_flags & UPS_PRINT_GRAPH) {
        m_graph << "}" << std::endl;

        std::ofstream file;
        file.open("graph.dot");
        file << m_graph.str();
      }
#endif
    }

  private:
    // Verifies a whole level in the tree - start with "page" and traverse
    // the linked list of all the siblings
    void verify_level(Page *parent, Page *page, uint32_t level) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->lenv();
      Page *child, *leftsib = 0;
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      // assert that the parent page's smallest item (item 0) is bigger
      // than the largest item in this page
      if (parent && node->get_left()) {
        int cmp = compare_keys(db, page, 0, node->get_count() - 1);
        if (cmp <= 0) {
          ups_log(("integrity check failed in page 0x%llx: parent item "
                  "#0 <= item #%d\n", page->get_address(),
                  node->get_count() - 1));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
      }

      m_children.clear();

      while (page) {
        // verify the page
        verify_page(parent, leftsib, page, level);

        // follow the right sibling
        BtreeNodeProxy *node = m_btree->get_node_from_page(page);
        if (node->get_right())
          child = env->page_manager()->fetch(m_context,
                          node->get_right(), PageManager::kReadOnly);
        else
          child = 0;

        if (leftsib) {
          BtreeNodeProxy *leftnode = m_btree->get_node_from_page(leftsib);
          ups_assert(leftnode->is_leaf() == node->is_leaf());
        }

        leftsib = page;
        page = child;
      }
    }

    // Verifies a single page
    void verify_page(Page *parent, Page *leftsib, Page *page, uint32_t level) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->lenv();
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

#if UPS_DEBUG
      if (m_flags & UPS_PRINT_GRAPH) {
        std::stringstream ss;
        ss << "node" << page->get_address();
        m_graph << "  \"" << ss.str() << "\" [" << std::endl
                << "    label = \"";
        m_graph << "<fl>L|<fd>D|";
        for (uint32_t i = 0; i < node->get_count(); i++) {
          m_graph << "<f" << i << ">" << i << "|";
        }
        m_graph << "<fr>R\"" << std::endl
                << "    shape = \"record\"" << std::endl
                << "  ];" << std::endl;
#if 0
        // edge to the left sibling
        if (node->get_left())
          m_graph << "\"" << ss.str() << "\":fl -> \"node"
                << node->get_left() << "\":fr [" << std::endl
                << "  ];" << std::endl;
        // to the right sibling
        if (node->get_right())
          m_graph << "  \"" << ss.str() << "\":fr -> \"node"
                << node->get_right() << "\":fl [" << std::endl
                << "  ];" << std::endl;
#endif
        // to ptr_down
        if (node->get_ptr_down())
          m_graph << "  \"" << ss.str() << "\":fd -> \"node"
                << node->get_ptr_down() << "\":fd [" << std::endl
                << "  ];" << std::endl;
        // to all children
        if (!node->is_leaf()) {
          for (uint32_t i = 0; i < node->get_count(); i++) {
            m_graph << "  \"" << ss.str() << "\":f" << i << " -> \"node"
                    << node->get_record_id(m_context, i) << "\":fd ["
                    << std::endl << "  ];" << std::endl;
          }
        }
      }
#endif

      if (node->get_count() == 0) {
        // a rootpage can be empty! check if this page is the rootpage
        if (page->get_address() == m_btree->root_address())
          return;

        // for internal nodes: ptr_down HAS to be set!
        if (!node->is_leaf() && node->get_ptr_down() == 0) {
          ups_log(("integrity check failed in page 0x%llx: empty page!\n",
                  page->get_address()));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
      }

      // check if the largest item of the left sibling is smaller than
      // the smallest item of this page
      if (leftsib) {
        BtreeNodeProxy *sibnode = m_btree->get_node_from_page(leftsib);
        ups_key_t key1 = {0};
        ups_key_t key2 = {0};

        node->check_integrity(m_context);

        if (node->get_count() > 0 && sibnode->get_count() > 0) {
          sibnode->get_key(m_context, sibnode->get_count() - 1,
                          &m_barray1, &key1);
          node->get_key(m_context, 0, &m_barray2, &key2);

          int cmp = node->compare(&key1, &key2);
          if (cmp >= 0) {
            ups_log(("integrity check failed in page 0x%llx: item #0 "
                    "< left sibling item #%d\n", page->get_address(),
                    sibnode->get_count() - 1));
            throw Exception(UPS_INTEGRITY_VIOLATED);
          }
        }
      }

      if (node->get_count() == 1)
        return;

      node->check_integrity(m_context);

      if (node->get_count() > 0) {
        for (uint32_t i = 0; i < node->get_count() - 1; i++) {
          int cmp = compare_keys(db, page, (uint32_t)i, (uint32_t)(i + 1));
          if (cmp >= 0) {
            ups_log(("integrity check failed in page 0x%llx: item #%d "
                    "< item #%d", page->get_address(), i, i + 1));
            throw Exception(UPS_INTEGRITY_VIOLATED);
          }
        }
      }

      // internal nodes: make sure that all record IDs are unique
      if (!node->is_leaf()) {
        if (m_children.find(node->get_ptr_down()) != m_children.end()) {
          ups_log(("integrity check failed in page 0x%llx: record of item "
                  "-1 is not unique", page->get_address()));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }
        m_children.insert(node->get_ptr_down());

        for (uint32_t i = 0; i < node->get_count(); i++) {
          uint64_t child_id = node->get_record_id(m_context, i);
          if (m_children.find(child_id) != m_children.end()) {
            ups_log(("integrity check failed in page 0x%llx: record of item "
                    "#%d is not unique", page->get_address(), i));
            throw Exception(UPS_INTEGRITY_VIOLATED);
          }
          PageManagerTest test = env->page_manager()->test();
          if (test.is_page_free(child_id)) {
            ups_log(("integrity check failed in page 0x%llx: record of item "
                    "#%d is in freelist", page->get_address(), i));
            throw Exception(UPS_INTEGRITY_VIOLATED);
          }
          m_children.insert(child_id);
       }
      }
    }

    int compare_keys(LocalDatabase *db, Page *page, int lhs, int rhs) {
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      ups_key_t key1 = {0};
      ups_key_t key2 = {0};

      node->get_key(m_context, lhs, &m_barray1, &key1);
      node->get_key(m_context, rhs, &m_barray2, &key2);

      return (node->compare(&key1, &key2));
    }

    // The BtreeIndex on which we operate
    BtreeIndex *m_btree;

    // The current Context
    Context *m_context;

    // The flags as specified when calling ups_db_check_integrity
    uint32_t m_flags;

    // ByteArrays to avoid frequent memory allocations
    ByteArray m_barray1;
    ByteArray m_barray2;

    // For checking uniqueness of record IDs on an internal level
    std::set<uint64_t> m_children;

#if UPS_DEBUG
    // For printing the graph
    std::ostringstream m_graph;
#endif
};

void
BtreeIndex::check_integrity(Context *context, uint32_t flags)
{
  BtreeCheckAction bta(this, context, flags);
  bta.run();
}

} // namespace upscaledb
