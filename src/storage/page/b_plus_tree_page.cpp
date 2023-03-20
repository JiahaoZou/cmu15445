//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
auto BPlusTreePage::IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { 
    return size_;
}
void BPlusTreePage::SetSize(int size) {
    size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
    size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { 
    
    return max_size_; 
}
void BPlusTreePage::SetMaxSize(int size) {
    max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
/**
 * 这个函数很特殊，是判断当前节点做删除操作时是否是处于安全状态的依据。
 * 不同的节点类型minsize不同，
 * 1.根节点，如果根节点也是叶节点，即此时只有一层，则根节点可以只有一个key-value对，放在第一个槽
 * 若根节点不为叶节点，此时根节点是一个特殊的中间节点，其第一个槽的key不存储数据，第一个槽的value则
 * 存储小于第二个槽的key的索引号，第二个槽的value存储大于等于第二个key的节点索引号。根节点
 * 的特殊之处在于他可以只有两个槽有效
 * 2.中间节点，最小得有max page size / 2个
 * 
 * 
 * 之所以这样做是因为可以方便我们分裂的处理，比如我们这个page的元素数量已经是max_size-1了，
 * 然后再插入一个，把新插入的存到我们的page里面，插入完成之后我们检查发现，
 * 这个page的size等于max_size，那么我们就可以直接把这个page传给一个split函数，让它进行分裂就好了。
 * 如果最后一个不充当哨兵的话，那么就会存在再插入的时候这个page已经放不下的情况，能处理，但相对没那么优雅。
*/
auto BPlusTreePage::GetMinSize() const -> int { 
    //区分是否为根节点
    if(IsRootPage()){
        return IsLeafPage()?1:2;
    }
    //最后世界存储的其实都是max_size-1，因为最后一个留做哨兵
    if(IsLeafPage()){
        return (max_size_ - 1 + 1) / 2;
    }
    //中间节点
    return (max_size_ - 1) / 2; 
}

/*
 * Helper methods to get/set parent page id
 */
auto BPlusTreePage::GetParentPageId() const -> page_id_t { 
    return parent_page_id_; 
}
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
    parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
auto BPlusTreePage::GetPageId() const -> page_id_t { 
    return page_id_; 
}
void BPlusTreePage::SetPageId(page_id_t page_id) {
    page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { 
    lsn_ = lsn; 
}

}  // namespace bustub
