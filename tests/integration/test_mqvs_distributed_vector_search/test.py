import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# shard1
node_s1_r1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s1_r2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

# shard2
node_s2_r1 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s2_r2 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node_s1_r1, node_s1_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(
                    id UInt32,
                    vector Array(Float32),
                    CONSTRAINT check_length CHECK length(vector) = 5
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_1/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )
        
        for node in [node_s2_r1, node_s2_r2]:
            node.query(
                """
                CREATE TABLE IF NOT EXISTS local_table(
                    id UInt32,
                    vector Array(Float32),
                    CONSTRAINT check_length CHECK length(vector) = 5
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_2/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

        node_s1_r1.query("CREATE TABLE distributed_table(id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = Distributed(test_cluster, default, local_table, id);")
        node_s1_r1.query("INSERT INTO distributed_table (id, vector) SELECT number, [number, number, number, number, number] FROM numbers(1, 100);")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD VECTOR INDEX vec_ind vector TYPE FLAT;")

        # Create a MergeTree table with the same data as baseline.
        node_s1_r1.query("CREATE TABLE test_table (id UInt32, vector Array(Float32), CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = MergeTree ORDER BY id;")
        node_s1_r1.query("INSERT INTO test_table (id, vector) SELECT number, [number, number, number, number, number] FROM numbers(1, 100);")
        node_s1_r1.query("ALTER TABLE test_table ADD VECTOR INDEX vec_ind vector TYPE FLAT;")

        yield cluster

    finally:
        cluster.shutdown()

def test_distributed_vector_search(started_cluster):

    def is_vector_index_status_ready(table_name):
        return node_s1_r1.query("SELECT status FROM system.vector_indices WHERE table = '{}'".format(table_name)) == "Built\n"

    count = 0
    while not is_vector_index_status_ready("test_table"):
        count += 1
        if count > 10:
            break
        time.sleep(1)

    count = 0
    while not is_vector_index_status_ready("local_table"):
        count += 1
        if count > 10:
            break
        time.sleep(1)

    assert node_s1_r1.query("SELECT status FROM system.vector_indices WHERE table = '{}'".format("test_table")) == "Built\n"
    assert node_s1_r1.query("SELECT status FROM system.vector_indices WHERE table = '{}'".format("local_table")) == "Built\n"

    distance_query = "SELECT id, distance(vector, [10., 20., 30., 40., 50.]) AS dist FROM {} ORDER BY dist ASC, id ASC LIMIT 5;"
    batch_distance_query = "SELECT id, batch_distance(vector, [[10., 20., 30., 40., 50.], [20., 30., 40., 50., 60.]]) AS dist FROM {} ORDER BY dist ASC, id ASC LIMIT 5 BY dist.1;"

    assert node_s1_r1.query(distance_query.format("test_table")) == node_s1_r1.query(distance_query.format("distributed_table"))
    assert node_s1_r1.query(batch_distance_query.format("test_table")) == node_s1_r1.query(batch_distance_query.format("distributed_table"))
