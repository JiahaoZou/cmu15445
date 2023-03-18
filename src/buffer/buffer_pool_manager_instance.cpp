//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*
  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");
      */
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
//创建一个新页面在缓冲池中，磁盘上没有
/** 通常是外部用bmp创建一个page，然后往page里面写数据。bmp需要找一个空闲的frame或从replacer中换出得到一个frame。
 * 然后调用AllocatePage生成一个unique的page id。这时候这个frame就是被访问到了，要在replacer中记录。此外，还要更新page_table_。*/
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::lock_guard<std::mutex> guard(latch_);
  
  frame_id_t frame_id;
  //先从空闲链表中拿一块frame
  if(!free_list_.empty()){
    frame_id = free_list_.back();
    free_list_.pop_back();
  }else{
    //free_list_中没有空闲frame了
    //从replacer中淘汰一块
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    if(pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
      //写回磁盘了，要更新为非脏状态
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  //到这一步，已经拿到frame号了，要为其分配新新的pageID
  *page_id = AllocatePage();
  pages_[frame_id].page_id_ = *page_id;
  //新创的页，磁盘上没有，当然是脏的
  pages_[frame_id].is_dirty_ = true;
  pages_[frame_id].pin_count_ = 1;
  //清空数据
  pages_[frame_id].ResetMemory();
  //更新page_table
  page_table_->Insert(pages_[frame_id].page_id_,frame_id);
  //在replacer中记录
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);

  return &pages_[frame_id]; 
}
//从缓冲池中拿一个页面，没有则从磁盘中取
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::lock_guard<std::mutex> guard(latch_);
  
  //映射由一个可拓展hash表实现，维护page_id_t --> frame_id_t的映射，
  //page_id_t 是磁盘上页的编号
  //frame_id_t是缓存中框的编号
  frame_id_t frame_id;
  
  if(page_table_->Find(page_id,frame_id)){
    //找到了映射，
    pages_[frame_id].pin_count_++;
    //一般而言recorAccess后紧跟setEvictable，所以在recordAcess中不需要对evictable做特别的处理
    //或者默认为false就好了
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return &pages_[frame_id];
  }

  //未在pages中找到,说明不在缓存中
  //接下来的操作：
  //1。查找空闲链表，如果空闲链表非空，说明缓冲区未满，在链表尾取下一个frame_id
  //2.如果空闲链表为空，此时就要执行淘汰策略，要用replacer来选择一个可以被evict的页返回。
  //
  if(!free_list_.empty()){
    frame_id  =  free_list_.back();
    free_list_.pop_back();
  }else{
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    if(pages_[frame_id].IsDirty()){
      disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
      //写会磁盘了，要更新为非脏状态
      //注意，脏状态是内存中的frame用来记录page状态的，磁盘中的原始数据是没有的
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  }
  //注意，这里有可能想要查找的page_id超过了磁盘的最大值
  //在本方法中不做处理，disk_manager的readPage方法会自己报错
  page_table_->Insert(page_id,frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  //从磁盘中读数据
  disk_manager_->ReadPage(page_id,pages_[frame_id].data_);
  
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);

  return &pages_[frame_id]; 
}
/** 这个接口是外部通过bmp显式调用的。当一个线程对某个page的操作结束，就调用UnpinPgImp，告诉bmp这个page我不用了，
 * 如果一个page没有线程在pinned（使用），就可以设置成evictable了，说明可以从缓冲池中换出了。
 * 第二个参数表示在该线程使用期间是否修改了该页*/
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::lock_guard<std::mutex> guard(latch_);
  //如果该页面不在内存中返回false
  frame_id_t frame_id;
  if(!page_table_->Find(page_id,frame_id)){
    return false;
  }
  //找到了
  pages_[frame_id].pin_count_--;
  if(is_dirty){
    pages_[frame_id].is_dirty_ = true;
  }
  if(pages_[frame_id].pin_count_<=0){
    //设置为可被驱逐
    replacer_->SetEvictable(frame_id,true);
  }
  return true; 
}
//刷盘
auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> guard(latch_);
  //首先判断对应页是否在缓冲中
  frame_id_t frame_id;
  if(!page_table_->Find(page_id,frame_id)){
    return true;
  }
  //找到了
  disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true; 
}
//刷盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for(size_t i=0;i<pool_size_;i++){
    if(pages_[i].page_id_==INVALID_PAGE_ID){
      continue;
    }
    disk_manager_->WritePage(pages_[i].GetPageId(),pages_[i].GetData());
  }
}
/** 删除Page，不是从buffer中删除，而是直接从磁盘中删除，
 * 如果page是non-evictable的则删除失败。注意要删除该page对应的frame在replacer中的记录。*/
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  //如果page没在缓冲池中，返回true即可
  if(!page_table_->Find(page_id,frame_id)){
    return true;
  }
  //查看pin数目，大于0， 不可删除，返回false
  if(pages_[frame_id].pin_count_>0){
    return false;
  }
  /** After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.*/
  //从replacer中移除
  replacer_->Remove(frame_id);
  //将frame加入freelist,取的时候从队尾，放回的时候就从队头
  free_list_.push_front(frame_id);
  //重置page的元数据
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].ResetMemory();
  //
  DeallocatePage(page_id);
  
  return true; 
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { 
  
  return next_page_id_++; 
}

}  // namespace bustub
