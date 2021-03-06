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
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef UPS_PAGE_H
#define UPS_PAGE_H

#include <string.h>
#include <stdint.h>
#include <boost/atomic.hpp>

#include "1base/error.h"
#include "1base/spinlock.h"
#include "1mem/mem.h"

namespace upscaledb {

class Device;
class BtreeCursor;
class BtreeNodeProxy;
class LocalDatabase;

#include "1base/packstart.h"

/*
 * This header is only available if the (non-persistent) flag
 * kNpersNoHeader is not set! Blob pages do not have this header.
 */
typedef UPS_PACK_0 struct UPS_PACK_1 PPageHeader {
  // flags of this page - currently only used for the Page::kType* codes
  uint32_t flags;

  // PRO: crc32
  uint32_t crc32;

  // the lsn of the last operation (unused)
  uint64_t lsn;

  // the persistent data blob
  uint8_t payload[1];

} UPS_PACK_2 PPageHeader;

#include "1base/packstop.h"

#include "1base/packstart.h"

/*
 * A union combining the page header and a pointer to the raw page data.
 *
 * This structure definition is present outside of @ref Page scope
 * to allow compile-time OFFSETOF macros to correctly judge the size,
 * depending on platform and compiler settings.
 */
typedef UPS_PACK_0 union UPS_PACK_1 PPageData {
  // the persistent header
  struct PPageHeader header;

  // a char pointer to the allocated storage on disk
  uint8_t payload[1];

} UPS_PACK_2 PPageData;

#include "1base/packstop.h"

/*
 * The Page class
 *
 * Each Page instance is a node in several linked lists.
 * In order to avoid multiple memory allocations, the previous/next pointers
 * are part of the Page class (m_prev and m_next). Both fields are arrays
 * of pointers and can be used i.e. with m_prev[Page::kListBucket] etc.
 * (or with the methods defined below).
 */
class Page {
  public:
    // A wrapper around the persisted page data
    struct PersistedData {
      PersistedData()
        : address(0), size(0), is_dirty(false), is_allocated(false),
          is_without_header(false), raw_data(0) {
      }

      PersistedData(const PersistedData &other)
        : address(other.address), size(other.size), is_dirty(other.is_dirty),
          is_allocated(other.is_allocated),
          is_without_header(other.is_without_header), raw_data(other.raw_data) {
      }

      ~PersistedData() {
#ifdef UPS_DEBUG
        mutex.safe_unlock();
#endif
        if (is_allocated)
          Memory::release(raw_data);
        raw_data = 0;
      }

      // The spinlock is locked if the page is in use or written to disk
      Spinlock mutex;

      // address of this page - the absolute offset in the file
      uint64_t address;

      // the size of this page
      uint32_t size;

      // is this page dirty and needs to be flushed to disk?
      bool is_dirty;

      // Page buffer was allocated with malloc() (if not then it was mapped
      // with mmap)
      bool is_allocated;

      // True if page has no persistent header
      bool is_without_header;

      // the persistent data of this page
      PPageData *raw_data;
    };

    // Misc. enums
    enum {
      // sizeof the persistent page header
      kSizeofPersistentHeader = sizeof(PPageHeader) - 1,

      // instruct Page::alloc() to reset the page with zeroes
      kInitializeWithZeroes,
    };

    // The various linked lists (indices in m_prev, m_next)
    enum {
      // list of all cached pages
      kListCache              = 0,

      // list of all pages in a changeset
      kListChangeset          = 1,

      // a bucket in the hash table of the cache
      kListBucket             = 2,

      // array limit
      kListMax                = 3
    };

    // non-persistent page flags
    enum {
      // page->m_data was allocated with malloc, not mmap
      kNpersMalloc            = 1,

      // page has no header (i.e. it's part of a large blob)
      kNpersNoHeader          = 2
    };

    // Page types
    //
    // When large BLOBs span multiple pages, only their initial page
    // will have a valid type code; subsequent pages of this blog will store
    // the data as-is, so as to provide one continuous storage space
    enum {
      // unidentified db page type
      kTypeUnknown            =  0x00000000,

      // the header page: this is the first page in the environment (offset 0)
      kTypeHeader             =  0x10000000,

      // a B+tree root page
      kTypeBroot              =  0x20000000,

      // a B+tree node page
      kTypeBindex             =  0x30000000,

      // a page storing the state of the PageManager
      kTypePageManager        =  0x40000000,

      // a page which stores blobs
      kTypeBlob               =  0x50000000
    };

    // Default constructor
    Page(Device *device, LocalDatabase *db = 0);

    // Destructor - releases allocated memory and resources, but neither
    // flushes dirty pages to disk nor moves them to the freelist!
    // Asserts that no cursors are attached.
    ~Page();

    // Returns the size of the usable persistent payload of a page
    // (page_size minus the overhead of the page header)
    static uint32_t usable_page_size(uint32_t raw_page_size) {
      return (raw_page_size - Page::kSizeofPersistentHeader);
    }


    // Returns the database which manages this page; can be NULL if this
    // page belongs to the Environment (i.e. for freelist-pages)
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Sets the database to which this Page belongs
    void set_db(LocalDatabase *db) {
      m_db = db;
    }

    // Returns the spinlock
    Spinlock &mutex() {
      return (m_datap->mutex);
    }

    // Returns the device
    Device *device() {
      return (m_device);
    }

    // Returns true if this is the header page of the Environment
    bool is_header() const {
      return (m_datap->address == 0);
    }

    // Returns the address of this page
    uint64_t get_address() const {
      return (m_datap->address);
    }

    // Sets the address of this page
    void set_address(uint64_t address) {
      m_datap->address = address;
    }

    // Returns true if this page is dirty (and needs to be flushed to disk)
    bool is_dirty() const {
      return (m_datap->is_dirty);
    }

    // Sets this page dirty/not dirty
    void set_dirty(bool dirty) {
      m_datap->is_dirty = dirty;
    }

    // Returns true if the page's buffer was allocated with malloc
    bool is_allocated() const {
      return (m_datap->is_allocated);
    }

    // Returns true if the page has no persistent header
    bool is_without_header() const {
      return (m_datap->is_without_header);
    }

    // Sets a flag whether the page has no persistent header
    void set_without_header(bool without_header) {
      m_datap->is_without_header = without_header;
    }

    // Assign a buffer which was allocated with malloc()
    void assign_allocated_buffer(void *buffer, uint64_t address) {
      m_datap->raw_data = (PPageData *)buffer;
      m_datap->is_allocated = true;
      m_datap->address = address;
    }

    // Assign a buffer from mmapped storage
    void assign_mapped_buffer(void *buffer, uint64_t address) {
      m_datap->raw_data = (PPageData *)buffer;
      m_datap->is_allocated = false;
      m_datap->address = address;
    }

    // Free resources associated with the buffer
    void free_buffer();

    // Returns the linked list of coupled cursors (can be NULL)
    BtreeCursor *cursor_list() {
      return (m_cursor_list);
    }

    // Sets the (head of the) linked list of cursors
    void set_cursor_list(BtreeCursor *cursor) {
      m_cursor_list = cursor;
    }

    // Returns the page's type (kType*)
    uint32_t get_type() const {
      return (m_datap->raw_data->header.flags);
    }

    // Sets the page's type (kType*)
    void set_type(uint32_t type) {
      m_datap->raw_data->header.flags = type;
    }

    // PRO: Returns the crc32
    uint32_t get_crc32() const {
      return (m_datap->raw_data->header.crc32);
    }

    // PRO: Sets the crc32
    void set_crc32(uint32_t crc32) {
      m_datap->raw_data->header.crc32 = crc32;
    }

    // Sets the pointer to the persistent data
    void set_data(PPageData *data) {
      m_datap->raw_data = data;
    }

    // Returns the pointer to the persistent data wrapper structure
    PersistedData *get_persisted_data() {
      return (m_datap);
    }

    // Returns the pointer to the persistent data
    // TODO required?
    PPageData *get_data() {
      return (m_datap->raw_data);
    }

    // Returns the persistent payload (after the header!)
    uint8_t *get_payload() {
      return (m_datap->raw_data->header.payload);
    }
    
    // Returns the persistent payload (after the header!)
    const uint8_t *get_payload() const {
      return (m_datap->raw_data->header.payload);
    }

    // Returns the persistent payload (including the header!)
    uint8_t *get_raw_payload() {
      return (m_datap->raw_data->payload);
    }

    // Returns the persistent payload (including the header!)
    const uint8_t *get_raw_payload() const {
      return (m_datap->raw_data->payload);
    }

    // Allocates a new page from the device
    // |flags|: either 0 or kInitializeWithZeroes
    void alloc(uint32_t type, uint32_t flags = 0);

    // Reads a page from the device
    void fetch(uint64_t address);

    // Writes a page to the device
    static void flush(Device *device, PersistedData *page_data);

    // Creates a deep copy of the persisted data; returns the old pointer
    // IF the old pointer was allocated and needs to be released
    PersistedData *deep_copy_data();

    // Returns true if the page's data already was "deep copied"
    bool has_deep_copied_data() const {
      return (m_datap != &m_data_inline);
    }

    // Returns true if this page is in a linked list
    bool is_in_list(Page *list_head, int list) {
      if (get_next(list) != 0)
        return (true);
      if (get_previous(list) != 0)
        return (true);
      return (list_head == this);
    }

    // Inserts this page at the beginning of a list and returns the
    // new head of the list
    Page *list_insert(Page *list_head, int list) {
      set_next(list, 0);
      set_previous(list, 0);

      if (!list_head)
        return (this);

      set_next(list, list_head);
      list_head->set_previous(list, this);
      return (this);
    }

    // Removes this page from a list and returns the new head of the list
    Page *list_remove(Page *list_head, int list) {
      Page *n, *p;

      if (this == list_head) {
        n = get_next(list);
        if (n)
          n->set_previous(list, 0);
        set_next(list, 0);
        set_previous(list, 0);
        return (n);
      }

      n = get_next(list);
      p = get_previous(list);
      if (p)
        p->set_next(list, n);
      if (n)
        n->set_previous(list, p);
      set_next(list, 0);
      set_previous(list, 0);
      return (list_head);
    }

    // Returns the next page in a linked list
    Page *get_next(int list) {
      return (m_next[list]);
    }

    // Returns the previous page of a linked list
    Page *get_previous(int list) {
      return (m_prev[list]);
    }

    // Returns the cached BtreeNodeProxy
    BtreeNodeProxy *get_node_proxy() {
      return (m_node_proxy);
    }

    // Sets the cached BtreeNodeProxy
    void set_node_proxy(BtreeNodeProxy *proxy) {
      m_node_proxy = proxy;
    }

    // tracks number of flushed pages
    static uint64_t ms_page_count_flushed;

  private:
    friend class PageCollection;

    // Sets the previous page of a linked list
    void set_previous(int list, Page *other) {
      m_prev[list] = other;
    }

    // Sets the next page in a linked list
    void set_next(int list, Page *other) {
      m_next[list] = other;
    }

    // the Device for allocating storage
    Device *m_device;

    // the Database handle (can be NULL)
    LocalDatabase *m_db;

    // linked list of all cursors which are coupled to that page
    BtreeCursor *m_cursor_list;

    // linked lists of pages - see comments above
    Page *m_prev[Page::kListMax];
    Page *m_next[Page::kListMax];

    // the cached BtreeNodeProxy object
    BtreeNodeProxy *m_node_proxy;

    // the persistent data of this page
    PersistedData *m_datap;
    PersistedData m_data_inline;
};

} // namespace upscaledb

#endif /* UPS_PAGE_H */
