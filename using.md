compile:
conan build -s:h build_type=RelWithDebInfo -c tools.build:skip_test=True -u -b missing ..

test:
./homestore_test_pg --gtest_filter=HomeObjectFixture.PGRecoveryWithDiskLostTest_2 --log_mods homeobject:trace,replication:trace > generated/1.log


