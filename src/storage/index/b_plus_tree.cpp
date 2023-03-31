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
  // std::cout << "进入getvalue  " << key << std::endl;
  if (IsEmpty()) {
    return false;
  }

  Page *page = FindLeafPage(key, transaction, READ);
  if (page == nullptr) {
    return false;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf_page->KeyIndex(key, comparator_);
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    if (transaction != nullptr) {
      UnlockAndUnpin(transaction, READ);
    } else {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    }

    return true;
  }
  if (transaction != nullptr) {
    UnlockAndUnpin(transaction, READ);
  } else {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  return false;
}
// 找叶节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction, Operation op) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // 这个while是为了寻找正确的rootpage
  // 为什么fetchpage操作无法保证寻找到正确的rootpage呢？
  // 因为我们在调用findLeafpage时是没有对整个b+树上锁的，b+树在并发的情况下可能会发生这种情况：
  // 进程1调用fetchpage取得了rootpage，但进程2马上修改了b+t的结构，导致rootid更新了。此时我们已经取得的rootpage自然已经不是rootpage
  while (true) {
    // std::cout << "循环find op" << op << std::endl;
    if (curr_page == nullptr) {
      return nullptr;
    }
    if (op == READ) {
      curr_page->RLatch();
    } else {
      curr_page->WLatch();
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(curr_page);
    }
    if (root_page_id_ == curr_page->GetPageId()) {
      break;
    }
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
    // 获取新的rootpage
    curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_inter->IsLeafPage()) {
    // 循环获取下一页，这里获取就没有获取根page那么麻烦，为什么？
    // 因为当前页是锁上的，下一页就不可能发生更改和竞争，直接获取就可以
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_inter->Lookup(key, comparator_));
    // 为了最大程度的保证并发性，应该选取适当的时机把之前经过的节点给unlock和unpin掉
    // 如果是读操作，只要是经过的节点都可以unlock掉，而不用管当前节点是否处于安全状态，因为读操作不会修改当前节点，故而也就不会影响之前的节点
    if (op == READ) {
      next_page->RLatch();
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }

    } else {
      // 如果是写操作，就要判断下一个节点是否是安全状态（即对下一个节点可能得修改是否会对当前节点产生影响：
      // 比如对下一个节点插入，是否会导致分裂并最终导致对当前节点插入
      // 又比如对下一个节点删除是否会导致合并，最终导致当前节点也删除元素）
      // 如果下一个节点是安全的，就能保证对下一个节点的操作不会对当前节点造成影响，也就更不会对当前节点之前的节点造成影响
      // 之前经过的所有不确定是否会被影响的节点全部记录在事务的set中，确认下一个节点安全，就可以将当前节点以及之前节点算不全部unpin和unlock掉
      next_page->WLatch();
      if (IsSafe(next_page, op)) {
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
  Page *page_leaf = FindLeafPage(key, transaction, INSERT);
  // 树是一棵空树，要锁整个树结构
  while (page_leaf == nullptr) {
    latch_.lock();
    if (IsEmpty()) {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(&page_id);
      auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page_id_ = page_id;
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    latch_.unlock();
    // 重新获取待插入的叶节点
    page_leaf = FindLeafPage(key, transaction, INSERT);
  }
  auto leaf_node = reinterpret_cast<LeafPage *>(page_leaf->GetData());
  // 二分法在叶节点中查找插入位置
  int index = leaf_node->KeyIndex(key, comparator_);
  bool retcode = leaf_node->Insert(std::make_pair(key, value), index, comparator_);
  if (!retcode) {
    UnlockAndUnpin(transaction, INSERT);
    return false;
  }
  // 插入成功了，判断是否安全
  if (leaf_node->GetSize() == leaf_max_size_) {
    // 节点分裂
    page_id_t page_bother_id;
    Page *page_bother = buffer_pool_manager_->NewPage(&page_bother_id);
    auto leaf_bother_node = reinterpret_cast<LeafPage *>(page_bother->GetData());
    leaf_bother_node->Init(page_bother_id, INVALID_PAGE_ID, leaf_max_size_);
    // 分裂， 将leaf的后半部分数据放到brother中
    leaf_node->Break(page_bother);
    // 分裂导致了向父节点中插入
    // 注意，很关键的一点，如果当前节点会分裂，那么就是处于不安全状态，那么它的父节点一定是上了锁的
    InsertInParent(page_leaf, leaf_bother_node->KeyAt(0), page_bother, transaction);
    buffer_pool_manager_->UnpinPage(page_bother->GetPageId(), true);
    UnlockAndUnpin(transaction, INSERT);
  }

  UnlockAndUnpin(transaction, INSERT);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(Page *page_leaf, const KeyType &key, Page *page_bother, Transaction *transaction)
    -> void {
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page_leaf->GetData());
  // 如果当前节点为最顶层，则需要创建一个新的根节点，整个b+t增高一层
  if (tree_page->GetParentPageId() == INVALID_PAGE_ID) {
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
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  page_id_t parent_id = tree_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto page_bother_node = reinterpret_cast<InternalPage *>(page_bother->GetData());
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
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
  InsertInParent(parent_page, parent_bother_node->KeyAt(0), page_parent_bother, transaction);
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
  Page *leaf_page = FindLeafPage(key, transaction, DELETE);
  if (leaf_page == nullptr) {
    return;
  }
  DeleteEntry(leaf_page, key, transaction);
  // 这些锁是在findleafpage过程中加的
  UnlockAndUnpin(transaction, DELETE);
}
// 按key来删除数据，本实验中保证了key是唯一的，但可不可以不唯一呢？比如对于不唯一的字段建索引
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Page *&page, const KeyType &key, Transaction *transaction) -> void {
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
  // 在当前页上成功将key对应的对删除
  // 当前节点为根节点
  if (root_page_id_ == b_node->GetPageId()) {
    // 当前节点为根节点，且为叶节点，且存储的数据大小等于0
    if (root_page_id_ == b_node->GetPageId() && b_node->IsLeafPage() && b_node->GetSize() == 0) {
      // 删除该节点，让树重新变为空树
      root_page_id_ = INVALID_PAGE_ID;
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());

      return;
    }
    // 根节点，且为中间节点，size最少为2，若只为1，说明其只有一个孩子，孩子应该为新的根，注意，此时孩子是没有锁的（因为已经解了），但其它线程（事务）也是无法操作孩子，因为当前节点上着锁
    if (root_page_id_ == b_node->GetPageId() && b_node->IsRootPage() && b_node->GetSize() == 1) {
      auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
      // 取孩子节点的id，先改变root_page_id节点的值。
      // 一旦改变，此时孩子节点就能够被访问到了。
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
  // 当前节点非根
  // 当前节点包含的数据量小于最小值，启动合并或是借用
  if (b_node->GetSize() < b_node->GetMinSize()) {
    Page *bother_page;
    KeyType parent_key;
    bool ispre;

    auto parent_page = (*transaction->GetPageSet())[transaction->GetPageSet()->size() - 2];
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    // 这里一定要非常注意，调用getbrotherpage时会返回一个ispre，如果是true表示此bro为当前节点的前一个兄弟，反之为后一个
    // 这里可以思考一个问题：当前所在节点为A, 在寻找叶节点的过程中兄弟节点是不会被加锁的，那么在取得A的前兄弟节点B时很可能B已经被别的进程加了锁，而恰巧另一个进程在占有B的同时也想找B的后兄弟节点A，此时就会造成死锁
    // 真的吗？
    // 回忆一下，在调用findleafpage时我们的解锁过程：只要判断了一个节点处于安全状态，其之前的节点全部可以解锁，反之如果一个节点不是安全状态，其父节点是不可能被解锁的
    // 如果A节点需要收缩，说明A节点是不安全的，则A的父节点P一定是不会解锁的，另一个线程占有B，也需要收缩，说明B也是不安全的，那么也不会解P的锁，这种情况也就是P上被加了两把锁，这是不可能的
    // 所以这里不用担心会发生死锁
    parent_node->GetBotherPage(page->GetPageId(), bother_page, parent_key, ispre, buffer_pool_manager_);
    auto bother_node = reinterpret_cast<BPlusTreePage *>(bother_page->GetData());
    // 两个加起来都太小了，只能合并
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

      DeleteEntry(parent_page, parent_key, transaction);

    } else {
      // 可以借用brother的，不用合并，只用调整
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
  // 迭代器在下降过程中上读锁，为什么？
  // 推荐是不要用迭代器进行修改操作
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_page_inter = reinterpret_cast<InternalPage *>(curr_page->GetData());
  // 找到最左的叶子节点
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
  // 找到指定的key所在的页
  auto leaf_page = FindLeafPage(key, nullptr, READ);

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
auto BPLUSTREE_TYPE::GetMaxsize(BPlusTreePage *page) const -> int {
  return page->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(Page *page, Operation op) -> bool {
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (op == INSERT) {
    return node->GetSize() < GetMaxsize(node);
  }
  if (node->GetParentPageId() == INVALID_PAGE_ID) {
    if (node->IsLeafPage()) {
      return true;
    }
    return node->GetSize() > 2;
  }
  return node->GetSize() > node->GetMinSize();
}
// 将指定事务的page集和deletepage集中page全部unpin或是delete
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


template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
