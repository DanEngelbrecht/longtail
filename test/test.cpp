#include "../third-party/jctest/src/jc_test.h"

#define LONGTAIL_IMPLEMENTATION
#include "../src/longtail.h"

struct TestStorage
{
    Longtail_ReadStorage m_Storge = {PreflightBlocks, AqcuireBlock, ReleaseBlock};
    static uint64_t PreflightBlocks(struct Longtail_ReadStorage* storage, uint32_t block_count, struct Longtail_BlockEntry* blocks)
    {
        TestStorage* test_storage = (TestStorage*)storage;
        return 0;
    }
    static struct Longtail_Block* AqcuireBlock(struct Longtail_ReadStorage* storage, meow_u128 block_hash)
    {
        TestStorage* test_storage = (TestStorage*)storage;
        return 0;
    }
    static void ReleaseBlock(struct Longtail_ReadStorage* storage, meow_u128 block_hash)
    {
        TestStorage* test_storage = (TestStorage*)storage;
    }
};

TEST(Longtail, Basic)
{
    TestStorage storage;
}
