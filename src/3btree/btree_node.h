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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_BTREE_NODE_H
#define UPS_BTREE_NODE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "3btree/btree_flags.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class PBtreeKeyDefault;

#include "1base/packstart.h"

/*
 * A BtreeNode structure spans the persistent part of a Page
 *
 * This structure is directly written to/read from the file.
 */
UPS_PACK_0 struct UPS_PACK_1 PBtreeNode
{
  public:
    // Result of the insert() operation
    struct InsertResult {
      InsertResult(ups_status_t _status = 0, int _slot = 0)
        : status(_status), slot(_slot) {
      }

      // upscaledb status code
      ups_status_t status;

      // the slot of the new (or existing) key
      int slot;
    };

    enum {
      // insert key at the beginning of the page
      kInsertPrepend = 1,

      // append key to the end of the page
      kInsertAppend = 2,
    };

    enum {
      // node is a leaf
      kLeafNode = 1
    };

    // Returns a PBtreeNode from a Page
    static PBtreeNode *from_page(Page *page) {
      return ((PBtreeNode *)page->get_payload());
    }

    // Returns the offset (in bytes) of the member |m_data|
    static uint32_t get_entry_offset() {
      return (sizeof(PBtreeNode) - 1);
    }

    // Returns the flags of the btree node (|kLeafNode|)
    uint32_t get_flags() const {
      return (m_flags);
    }

    // Sets the flags of the btree node (|kLeafNode|)
    void set_flags(uint32_t flags) {
      m_flags = flags;
    }

    // Returns the number of entries in a BtreeNode
    uint32_t get_count() const {
      return (m_count);
    }

    // Sets the number of entries in a BtreeNode
    void set_count(uint32_t count) {
      m_count = count;
    }

    // Returns the address of the left sibling of this node
    uint64_t get_left() const {
      return (m_left);
    }

    // Sets the address of the left sibling of this node
    void set_left(uint64_t left) {
      m_left = left;
    }

    // Returns the address of the right sibling of this node
    uint64_t get_right() const {
      return (m_right);
    }

    // Sets the address of the right sibling of this node
    void set_right(uint64_t right) {
      m_right = right;
    }

    // Returns the ptr_down of this node
    uint64_t get_ptr_down() const {
      return (m_ptr_down);
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (m_flags & kLeafNode);
    }

    // Sets the ptr_down of this node
    void set_ptr_down(uint64_t ptr_down) {
      m_ptr_down = ptr_down;
    }

    // Returns a pointer to the key data
    uint8_t *get_data() {
      return (&m_data[0]);
    }

    const uint8_t *get_data() const {
      return (&m_data[0]);
    }

  private:
    // flags of this node
    uint32_t m_flags;

    // number of used entries in the node
    uint32_t m_count;
  
    // address of left sibling
    uint64_t m_left;

    // address of right sibling
    uint64_t m_right;

    // address of child node whose items are smaller than all items
    // in this node
    uint64_t m_ptr_down;

    // the entries of this node
    uint8_t m_data[1];

} UPS_PACK_2;

#include "1base/packstop.h"

} // namespace upscaledb

#endif /* UPS_BTREE_NODE_H */
