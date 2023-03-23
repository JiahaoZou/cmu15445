#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/** 
 * 测试文件中这样来创建一棵tree("foo_pk", bpm, comparator, 2, 3);
 * 由于中间节点的第一个key是不存储值的，所以我们可以认为，一个中间节点它的size初始时就为1，所以为了和叶节点存储数量同步，
 * 中间节点的maxsize会大1，同时，无论是中间节点还是叶节点，都应该保留最后一个位置为空，以便于分裂
 * 所以判断是否要分裂的情况为插入后 size == max_size 或 插入前 size == maxsize-1;
 * 根据测试样例来看：
 * 叶节点插入一个数据时大小为1，并且0号位是存值的
 * 在本项目中，锁是加在page上的，pages是缓冲区管理的一段连续内存空间，其中除了磁盘中的页上的数据（即data）外还有一些额外的控制
 * 信息，就包含脏位，pin和锁，一旦该页被换出内存，这些信息都被清空，锁以被锁住的页一定是不能被换回内存的，即一定是被pin住的。
 */
/**
 * For this task, you have to use the passed in pointer parameter called 
 * transaction (src/include/concurrency/transaction.h).
 * It provides methods to store the page on which you have acquired latch while traversing
 * through B+ tree and also methods to store the page which you have deleted during Remove
 * operation. Our suggestion is to look closely at the FindLeafPage method within B+ tree,
 * you may wanna modify your previous implementation (note that you may need to change to return
 * value for this method) and then add the logic of latch crabbing within this particular method.
 */

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 树为空，返回false
  if (IsEmpty()) {
    return false;
  }
  // 找key所在的叶节点
  Page *page = FindLeafPageRW(key, transaction, READ);
  if (page == nullptr) {
    return false;
  }
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // 找到key在page中的下标， TODO：查看KeyIndex具体实现
  int index = leaf_page->KeyIndex(key, comparator_);
  // 检测找到的index是否正确
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    if (transaction != nullptr) {
      UnlockAndUnpin(transaction, READ);
    } else {
      // 对page解锁，和unpin，且非脏
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    }

    return true;
  }
  // 没有找到正确的INDEX
  if (transaction != nullptr) {
    UnlockAndUnpin(transaction, READ);
  } else {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 树空，则创建一个新的根节点
  Page *page_leaf = FindLeafPage(key);
  /*std::cout << buffer_pool_manager_->GetPoolSize() << std::endl;
  std::cout << leaf_max_size_ << std::endl;
  std::cout << internal_max_size_ << std::endl;*/
  // std::cout << "进入Insert " << key << std::endl;
  // 没有找到叶节点，只有一种情况，就是树是空的
  while (page_leaf == nullptr) {
    // 树是空的，就对整个b+树索引结构上一个锁
    latch_.lock();
    // 为什么还要判断一次是否为空？
    // 因为找叶子节点和对树上锁不是原子的，我们调用FindLeaf时得到树为空，在我们对整个树结构上锁前，有可能别人先
    // 上锁，然后添加了根节点，使得树非空，而我们如果不再次进行判断，就会仍然认为树为空，为其重复添加根节点
    if (IsEmpty()) {
      page_id_t page_id;
      // 申请一个新的page存放新的节点
      Page *page = buffer_pool_manager_->NewPage(&page_id);
      LeafPage * leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page_id_ = page_id;
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    // std::cout << "INsert找头节点";
    latch_.unlock();
    page_leaf = FindLeafPage(key);
  }
  // 找到叶节点
  LeafPage * leaf_node = reinterpret_cast<LeafPage *>(page_leaf->GetData());
  // 找index应该插入的位置
  int index = leaf_node->KeyIndex(key, comparator_);
  // 在leafpage的insert中，会判断待插入的key与对应index的key是否相同，相同则返回false
  bool retcode = leaf_node->Insert(std::make_pair(key, value), index, comparator_);
  if (!retcode) {
    // UnlockAndUnpin(transaction, INSERT);
    return false;
  }
  // 判断插入一个点后大小是否等于最大大小
  // 所有节点的最后一个槽是不插key和value的，为了优雅的实现分裂
  if (!IsSafe(leaf_node, INSERT)) {
    // 启动分裂流程
    page_id_t page_bother_id;
    // 申请一块新内存
    Page *page_bother = buffer_pool_manager_->NewPage(&page_bother_id);
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(page_bother->GetData());
    leaf_bother_node->Init(page_bother_id, INVALID_PAGE_ID, leaf_max_size_);
    // 拆页，将后半的keyvalue存入brother中
    leaf_node->Break(page_bother);
    // 注意，这里是叶节点，第0个是存数据的
    // 要传两个指针，第一个参数是当前页的指针，第三个为bro页的指针，父节点需要保存着两个指针
    InsertInParent(page_leaf, leaf_bother_node->KeyAt(0), page_bother);
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
    // UnlockAndUnpin(transaction, INSERT);
  }

  // UnlockAndUnpin(transaction, INSERT);

  return true;
}
/**
 * 整个框架已经帮我们写好了实际管理磁盘的类，page是对磁盘页的抽象，page的-》getdate方法就是取得实际页的数据
 * 我们整个b+树的内容都是属于数据部分，包括头和键值对
 * 这里是将数据强制转换为leafpage结构
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  // 循环方式找leafpage，完全无加锁
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->Lookup(key, comparator_));
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  return curr_page;
}
// 利用集合实现
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPageRW(const KeyType &key, Transaction *transaction, Operation op) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  while (true) {
    // 为什么要循环？在并发情况下很可能缓冲区没有空间而取不到根节点，这时候如果返回false这个函数就意义不明了。
    while (curr_page == nullptr) {
      curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
    }
    // 根据操作类型加不同的锁
    if (op == READ) {
      curr_page->RLatch();
    } else {
      curr_page->WLatch();
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(curr_page);
    }
    // 如果正确找到根节点，则退出循环即可
    if (root_page_id_ == curr_page->GetPageId()) {
      break;
    }
    // 不是根节点，说明此时根节点已经改变了
    if (op == READ) {
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }

    } else {
      UnlockAndUnpin(transaction, op);
    }

    curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  // 总之，到这一步根节点找到了，并且加了锁
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  // 由这里可以看出，我们在创建第一个节点时（即根节点）要把它的类型设置为叶节点
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->Lookup(key, comparator_));
    // 这里有错误，解锁的前提是当前节点安全，即size < max_size - 1
    if (op == READ) {
      next_page->RLatch();
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }

    } else {
      next_page->WLatch();
      if (IsSafe(reinterpret_cast<BPlusTreePage*>(next_page->GetData()), op)) {
        UnlockAndUnpin(transaction, op);
      }
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(next_page);
    }

    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  return curr_page;
}

/**
 * 分裂时向parent中插值，进入这个函数就一定不是叶节点了
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(Page *page_leaf, const KeyType &key, Page *page_bother) -> void {
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());

  if (tree_page->GetParentPageId() == INVALID_PAGE_ID) {
    // 如果没有父节点
    // 创建一个父节点，作为新的根节点
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetValueAt(0, page_leaf->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, page_bother->GetPageId());
    new_root->IncreaseSize(2);
    auto page_leaf_node = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
    page_leaf_node->SetParentPageId(new_page_id);
    auto page_bother_node = reinterpret_cast<BPlusTreePage *>(page_bother->GetData());
    page_bother_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    // unpin掉parent
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  // 有父节点
  page_id_t parent_id = tree_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto page_bother_node = reinterpret_cast<InternalPage *>(page_bother->GetData());
  // 向parent中插值
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    // 判断是否满了，没有满
    parent_node->Insert(std::make_pair(key, page_bother->GetPageId()), comparator_);
    page_bother_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }
  page_id_t page_parent_bother_id;
  Page *page_parent_bother = buffer_pool_manager_->NewPage(&page_parent_bother_id);
  auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());
  parent_bother_node->Init(page_parent_bother_id, INVALID_PAGE_ID, internal_max_size_);
  parent_node->Break(key, page_bother, page_parent_bother, comparator_, buffer_pool_manager_);
  InsertInParent(parent_page, parent_bother_node->KeyAt(0), page_parent_bother);
  buffer_pool_manager_->UnpinPage(page_parent_bother_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParentRW(Page *page_leaf, const KeyType &key, Page *page_bother, Transaction *transaction)
    -> void {
  // std::cout << "进入insert递归";
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
  // 找不到父节点，说明此时为根节点，要new一个父节点
  if (tree_page->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    // 新new出来的根节点为中间节点，中间节点第0个key是没有的，第0个value是存放小于第一个key的page的id
    new_root->SetValueAt(0, page_leaf->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, page_bother->GetPageId());
    new_root->IncreaseSize(2);
    auto page_leaf_node = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
    page_leaf_node->SetParentPageId(new_page_id);
    auto page_bother_node = reinterpret_cast<BPlusTreePage *>(page_bother->GetData());
    page_bother_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  page_id_t parent_id = tree_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto page_bother_node = reinterpret_cast<InternalPage *>(page_bother->GetData());
  parent_node->Insert(std::make_pair(key, page_bother->GetPageId()), comparator_);
  // 未满
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    page_bother_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }
  // 满了，要继续将新的节点分裂
  page_id_t page_parent_bother_id;
  Page *page_parent_bother = buffer_pool_manager_->NewPage(&page_parent_bother_id);
  auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());
  parent_bother_node->Init(page_parent_bother_id, INVALID_PAGE_ID, internal_max_size_);
  parent_node->Break(key, page_bother, page_parent_bother, comparator_, buffer_pool_manager_);
  InsertInParentRW(parent_page, parent_bother_node->KeyAt(0), page_parent_bother, transaction);
  buffer_pool_manager_->UnpinPage(page_parent_bother_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page *leaf_page = FindLeafPage(key);
  if (leaf_page == nullptr) {
    return;
  }
  DeleteEntry(leaf_page, key);
  // UnlockAndUnpin(transaction, DELETE);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Page *&page, const KeyType &key) -> void {
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    // delete返回false的情况：没有找到要删除的节点，所以直接返回即可
    if (!leaf_node->Delete(key, comparator_)) {
      return;
    }
  } else {
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    if (!inter_node->Delete(key, comparator_)) {
      return;
    }
  }
  // 到这个地方，在当前节点的删除操作成功了
  // 如果当前节点为根节点，且为叶节点，且大小等于0，相当于树已经空了，则直接将节点删除，让树变为null的状态
  if (root_page_id_ == b_node->GetPageId() && b_node->IsLeafPage() && b_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(b_node->GetPageId());
    return;
  }
  // 如果此节点为根节点，且不为叶节点，即为中间节点，根节点作为中间节点，最少size得为2(得有一个key)，所以若为1，则应该删去
  // 该节点，由于作为中间节点，第一个key是没有值的，所以size为1意味着此时没有key，只有一个value，即value（0）
  // 这个value(0)作为根节点下一层的唯一一个page，理应成为新的根节点
  if (root_page_id_ == b_node->GetPageId() && b_node->IsRootPage() && b_node->GetSize() == 1) {
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    root_page_id_ = inter_node->ValueAt(0);
    buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(b_node->GetPageId());
    return;
  }
  // 到这里当前节点还有可能是根节点
  if (root_page_id_ == b_node->GetPageId()) {
    // 如果是根节点，则一定不会是不安全状态，返回就行了
    buffer_pool_manager_->UnpinPage(b_node->GetPageId(), true);
    return;
  }
  // 当前一定不会是根节点了
  if (b_node->GetSize() < b_node->GetMinSize()) {
    // 不安全，需要合并
    Page *bother_page;
    KeyType parent_key;
    bool ispre;
    // 通过父节点找兄弟节点，想想如果是叶节点能否直接通过指针找兄弟节点呢？
    // 不可以，因为通过指针找到的相邻节点不一定属于同一个父节点，即通过指针可以找相邻节点，但不一定找得了兄弟节点
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    auto parent_page_id = inter_node->GetParentPageId();
    auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_);

    auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
    // 两个node加起来的总大小小于最大值， 可以合并
    if ((bother_node->GetSize() + b_node->GetSize()) < GetMaxsize(b_node)) {
      // 如果bro不是当前节点的前一个，则将指针做一次交换
      if (!ispre) {
        auto tmp_page = page;
        page = bother_page;
        bother_page = tmp_page;
        auto tmp_node = b_node;
        b_node = bother_node;
        bother_node = tmp_node;
      }
      if (b_node->IsRootPage()) {
        // 是中间节点
        auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
        // auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
        inter_bother_node->Merge(parent_key, page, buffer_pool_manager_);
      } else {
        // 是叶节点
        auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
        auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
        leaf_bother_node->Merge(page, buffer_pool_manager_);
        leaf_bother_node->SetNextPageId(leaf_b_node->GetNextPageId());
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());
      buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
      DeleteEntry(parent_page, parent_key);

    } else {
      // 不安全，但不可以合并，即两边之和太大了
      if (ispre) {
        if (bother_node->IsRootPage()) {
          // 是中间节点
          auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
          auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
          // 将前一个节点的最后一个值借过来放在当前的节点
          page_id_t last_value = inter_bother_node->ValueAt(inter_bother_node->GetSize() - 1);
          KeyType last_key = inter_bother_node->KeyAt(inter_bother_node->GetSize() - 1);
          inter_bother_node->Delete(last_key, comparator_);
          // 插到最前边
          inter_b_node->InsertFirst(parent_key, last_value);
          // 前一个节点的最后一个孩子要改变parent
          // 为什么这里取节点时不pin，但之后却要unpin？
          auto child_page = buffer_pool_manager_->FetchPage(last_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          if (child_node->IsLeafPage()) {
            auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
          } else {
            auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            inter_child_node->SetParentPageId(inter_b_node->GetPageId());
          }
          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, last_key);
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);

        } else {
          // 当前节点为叶节点，相比中间节点，叶节点不需要调整孩子
          auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
          auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType last_value = leaf_bother_node->ValueAt(leaf_bother_node->GetSize() - 1);
          KeyType last_key = leaf_bother_node->KeyAt(leaf_bother_node->GetSize() - 1);
          leaf_bother_node->Delete(last_key, comparator_);
          leaf_b_node->InsertFirst(last_key, last_value);

          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, last_key);
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
        }
      } else {
        // 取得的bro为后一个节点，需要把bro的第一个节点给借过来
        if (bother_node->IsRootPage()) {
          // 是中间节点
          auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
          auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
          page_id_t first_value = inter_bother_node->ValueAt(0);
          KeyType first_key = inter_bother_node->KeyAt(1);
          inter_bother_node->DeleteFirst();
          inter_b_node->Insert(std::make_pair(parent_key, first_value), comparator_);
          auto child_page = buffer_pool_manager_->FetchPage(first_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          if (child_node->IsLeafPage()) {
            auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
          } else {
            auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            inter_child_node->SetParentPageId(inter_b_node->GetPageId());
          }

          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, first_key);
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);

        } else {
          auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
          auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType first_value = leaf_bother_node->ValueAt(0);
          KeyType first_key = leaf_bother_node->KeyAt(0);
          leaf_bother_node->Delete(first_key, comparator_);
          leaf_b_node->InsertLast(first_key, first_value);

          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, leaf_bother_node->KeyAt(0));
          buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
        }
      }
    }
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(0));
    next_page->RLatch();
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }

  return INDEXITERATOR_TYPE(curr_page, 0, curr_page->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto leaf_page = FindLeafPageRW(key, nullptr, READ);

  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index;
  for (index = 0; index < leaf_node->GetSize(); index++) {
    if (comparator_(leaf_node->KeyAt(index), key) == 0) {
      break;
    }
  }
  if (index == leaf_node->GetSize()) {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return End();
  }
  return INDEXITERATOR_TYPE(leaf_page, index, leaf_page->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->ValueAt(curr_page_inter->GetSize() - 1));
    next_page->RLatch();
    auto next_page_inter = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_page_inter = next_page_inter;
  }
  auto curr_node = reinterpret_cast<LeafPage *>(curr_page->GetData());

  /*  while (curr_node->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_id = curr_node->GetNextPageId();
      auto next_page = buffer_pool_manager_->FetchPage(next_id);
      next_page->RLatch();
      curr_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(curr_node->GetPageId(), false);
      curr_page = next_page;
      curr_node = reinterpret_cast<LeafPage *>(curr_page->GetData());
    }*/
  page_id_t page_id = curr_page->GetPageId();
  curr_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(curr_page, curr_node->GetSize(), page_id, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMaxsize(BPlusTreePage *page) const -> int {
  return page->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
}
// insert是判断插入后的状态， 而delete时是判断删除前的状态，调用时要注意时机
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *page, Operation op) -> bool {
  if (op == INSERT) {
    return page->GetSize() < GetMaxsize(page);
  }
  // delete
  if (page->GetParentPageId() == INVALID_PAGE_ID) {
    // 根
    if (page->IsLeafPage()) {
      return true;
    }
    return page->GetSize() > 2;
  }
  return page->GetSize() > page->GetMinSize();
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnlockAndUnpin(Transaction *transaction, Operation op) -> void {
  if (transaction == nullptr) {
    return;
  }
  for (auto page : *transaction->GetPageSet()) {
    if (op == READ) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();
  for (auto page : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntryRW(Page *&page, const KeyType &key, Transaction *transaction) -> void {
  // std::cout << "进入delete递归";
  auto b_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    if (!leaf_node->Delete(key, comparator_)) {
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return;
    }
  } else {
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    if (!inter_node->Delete(key, comparator_)) {
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return;
    }
  }

  if (root_page_id_ == b_node->GetPageId()) {
    if (root_page_id_ == b_node->GetPageId() && b_node->IsLeafPage() && b_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());

      return;
    }
    if (root_page_id_ == b_node->GetPageId() && b_node->IsRootPage() && b_node->GetSize() == 1) {
      auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
      root_page_id_ = inter_node->ValueAt(0);
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());
      return;
    }
    transaction->GetPageSet()->pop_back();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  if (b_node->GetSize() < b_node->GetMinSize()) {
    Page *bother_page;
    KeyType parent_key;
    bool ispre;

    // auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    // auto parent_page_id = inter_node->GetParentPageId();

    // auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    auto parent_page = (*transaction->GetPageSet())[transaction->GetPageSet()->size() - 2];
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // parent_node->GetBotherPageRW(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_,
    // transaction);
    parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_);
    auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
    if ((bother_node->GetSize() + b_node->GetSize()) <= GetMaxsize(b_node)) {
      if (!ispre) {
        auto tmp_page = page;
        page = bother_page;
        bother_page = tmp_page;
        auto tmp_node = b_node;
        b_node = bother_node;
        bother_node = tmp_node;
      }
      if (b_node->IsRootPage()) {
        auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
        // auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
        inter_bother_node->Merge(parent_key, page, buffer_pool_manager_);
        transaction->GetPageSet()->pop_back();
        bother_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(inter_bother_node->GetPageId(), true);
      } else {
        auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
        auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
        auto next_page_id = leaf_b_node->GetNextPageId();
        leaf_bother_node->Merge(page, buffer_pool_manager_);
        leaf_bother_node->SetNextPageId(next_page_id);
        transaction->GetPageSet()->pop_back();
        bother_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_bother_node->GetPageId(), true);
      }
      // transaction->AddIntoDeletedPageSet(page->GetPageId());

      DeleteEntryRW(parent_page, parent_key, transaction);
      // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

    } else {
      if (ispre) {
        if (bother_node->IsRootPage()) {
          auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
          auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
          page_id_t last_value = inter_bother_node->ValueAt(inter_bother_node->GetSize() - 1);
          KeyType last_key = inter_bother_node->KeyAt(inter_bother_node->GetSize() - 1);
          inter_bother_node->Delete(last_key, comparator_);

          bother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(inter_bother_node->GetPageId(), true);

          inter_b_node->InsertFirst(parent_key, last_value);

          auto child_page = buffer_pool_manager_->FetchPage(last_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          if (child_node->IsLeafPage()) {
            auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
          } else {
            auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            inter_child_node->SetParentPageId(inter_b_node->GetPageId());
          }
          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, last_key);
          // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

        } else {
          auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
          auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType last_value = leaf_bother_node->ValueAt(leaf_bother_node->GetSize() - 1);
          KeyType last_key = leaf_bother_node->KeyAt(leaf_bother_node->GetSize() - 1);
          leaf_bother_node->Delete(last_key, comparator_);
          leaf_b_node->InsertFirst(last_key, last_value);

          bother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(leaf_bother_node->GetPageId(), true);
          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, last_key);
          // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
      } else {
        if (bother_node->IsRootPage()) {
          auto inter_bother_node = reinterpret_cast<InternalPage *>(bother_page->GetData());
          auto inter_b_node = reinterpret_cast<InternalPage *>(page->GetData());
          page_id_t first_value = inter_bother_node->ValueAt(0);
          KeyType first_key = inter_bother_node->KeyAt(1);
          inter_bother_node->DeleteFirst();

          bother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);

          inter_b_node->Insert(std::make_pair(parent_key, first_value), comparator_);
          auto child_page = buffer_pool_manager_->FetchPage(first_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          if (child_node->IsLeafPage()) {
            auto leaf_child_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            leaf_child_node->SetParentPageId(inter_b_node->GetPageId());
          } else {
            auto inter_child_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            inter_child_node->SetParentPageId(inter_b_node->GetPageId());
          }

          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, first_key);
          // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);

        } else {
          auto leaf_bother_node = reinterpret_cast<LeafPage *>(bother_page->GetData());
          auto leaf_b_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType first_value = leaf_bother_node->ValueAt(0);
          KeyType first_key = leaf_bother_node->KeyAt(0);
          leaf_bother_node->Delete(first_key, comparator_);
          leaf_b_node->InsertLast(first_key, first_value);

          bother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(bother_page->GetPageId(), true);
          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          auto inter_parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = inter_parent_node->KeyIndex(parent_key, comparator_);
          inter_parent_node->SetKeyAt(index, leaf_bother_node->KeyAt(0));
          // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        }
      }
    }
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
