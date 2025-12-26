import pytest
import itertools
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# shard1
node_s1_r1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s1_r2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

# shard2
node_s2_r1 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)
node_s2_r2 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True)

data = [
        (1, [1.0, 1.0, 1.0, 1.0, 1.0], 'The mayor announced a new initiative to revitalize the downtown area. This project will include the construction of new parks and the renovation of historic buildings.', [(1, 1.4), (12, 1.6), (23, 2.1), (34, 2.6), (45, 3.1), (56, 3.6), (67, 4.1), (78, 4.6), (89, 5.1), (100, 5.6)]),
        (2, [2.0, 2.0, 2.0, 2.0, 2.0], 'Local schools are introducing a new curriculum focused on science and technology. The goal is to better prepare students for careers in STEM fields.', [(2, 1.4), (13, 1.6), (24, 2.1), (35, 2.6), (46, 3.1), (57, 3.6), (68, 4.1), (79, 4.6), (90, 5.1), (101, 5.6)]),
        (3, [3.0, 3.0, 3.0, 3.0, 3.0], 'A new community center is opening next month, offering a variety of programs for residents of all ages. Activities include fitness classes, art workshops, and social events.', [(3, 1.4), (14, 1.6), (25, 2.1), (36, 2.6), (47, 3.1), (58, 3.6), (69, 4.1), (80, 4.6), (91, 5.1), (102, 5.6)]),
        (4, [4.0, 4.0, 4.0, 4.0, 4.0], 'The city council has approved a plan to improve public transportation. This includes expanding bus routes and adding more frequent services during peak hours.', [(4, 1.4), (15, 1.6), (26, 2.1), (37, 2.6), (48, 3.1), (59, 3.6), (70, 4.1), (81, 4.6), (92, 5.1), (103, 5.6)]),
        (5, [5.0, 5.0, 5.0, 5.0, 5.0], 'A new library is being built in the west side of the city. The library will feature modern facilities, including a digital media lab and community meeting rooms.', [(5, 1.4), (16, 1.6), (27, 2.1), (38, 2.6), (49, 3.1), (60, 3.6), (71, 4.1), (82, 4.6), (93, 5.1), (104, 5.6)]),
        (6, [6.0, 6.0, 6.0, 6.0, 6.0], 'The local hospital has received funding to upgrade its emergency department. The improvements will enhance patient care and reduce wait times.', [(6, 1.4), (17, 1.6), (28, 2.1), (39, 2.6), (50, 3.1), (61, 3.6), (72, 4.1), (83, 4.6), (94, 5.1), (105, 5.6)]),
        (7, [7.0, 7.0, 7.0, 7.0, 7.0], 'A new startup accelerator has been launched to support tech entrepreneurs. The program offers mentoring, networking opportunities, and access to investors.', [(7, 1.4), (18, 1.6), (29, 2.1), (40, 2.6), (51, 3.1), (62, 3.6), (73, 4.1), (84, 4.6), (95, 5.1), (106, 5.6)]),
        (8, [8.0, 8.0, 8.0, 8.0, 8.0], 'The city is hosting a series of public workshops on climate change. The sessions aim to educate residents on how to reduce their carbon footprint.', [(8, 1.4), (19, 1.6), (30, 2.1), (41, 2.6), (52, 3.1), (63, 3.6), (74, 4.1), (85, 4.6), (96, 5.1), (107, 5.6)]),
        (9, [9.0, 9.0, 9.0, 9.0, 9.0], 'A popular local restaurant is expanding with a new location in the downtown area. The restaurant is known for its farm-to-table cuisine and sustainable practices.', [(9, 1.4), (20, 1.6), (31, 2.1), (42, 2.6), (53, 3.1), (64, 3.6), (75, 4.1), (86, 4.6), (97, 5.1), (108, 5.6)]),
        (10, [10.0, 10.0, 10.0, 10.0, 10.0], 'The annual arts festival is set to begin next week, featuring performances, exhibitions, and workshops by local and international artists.', [(10, 1.4), (21, 1.6), (32, 2.1), (43, 2.6), (54, 3.1), (65, 3.6), (76, 4.1), (87, 4.6), (98, 5.1), (109, 5.6)]),
        (11, [11.0, 11.0, 11.0, 11.0, 11.0], 'The city is implementing new measures to improve air quality, including stricter emissions standards for industrial facilities and incentives for electric vehicles.', [(11, 1.4), (22, 1.6), (33, 2.1), (44, 2.6), (55, 3.1), (66, 3.6), (77, 4.1), (88, 4.6), (99, 5.1), (110, 5.6)]),
        (12, [12.0, 12.0, 12.0, 12.0, 12.0], 'A local nonprofit is organizing a food drive to support families in need. Donations of non-perishable food items can be dropped off at designated locations.', [(12, 1.4), (23, 1.6), (34, 2.1), (45, 2.6), (56, 3.1), (67, 3.6), (78, 4.1), (89, 4.6), (100, 5.1), (111, 5.6)]),
        (13, [13.0, 13.0, 13.0, 13.0, 13.0], 'The community garden project is expanding, with new plots available for residents to grow their own vegetables and herbs. The garden promotes healthy eating and sustainability.', [(13, 1.4), (24, 1.6), (35, 2.1), (46, 2.6), (57, 3.1), (68, 3.6), (79, 4.1), (90, 4.6), (101, 5.1), (112, 5.6)]),
        (14, [14.0, 14.0, 14.0, 14.0, 14.0], 'The police department is introducing body cameras for officers to increase transparency and accountability. The initiative is part of broader efforts to build trust with the community.', [(14, 1.4), (25, 1.6), (36, 2.1), (47, 2.6), (58, 3.1), (69, 3.6), (80, 4.1), (91, 4.6), (102, 5.1), (113, 5.6)]),
        (15, [15.0, 15.0, 15.0, 15.0, 15.0], 'A new public swimming pool is opening this summer, offering swimming lessons and recreational activities for all ages. The pool is part of a larger effort to promote health and wellness.', [(15, 1.4), (26, 1.6), (37, 2.1), (48, 2.6), (59, 3.1), (70, 3.6), (81, 4.1), (92, 4.6), (103, 5.1), (114, 5.6)]),
        (16, [16.0, 16.0, 16.0, 16.0, 16.0], 'The city is launching a campaign to promote recycling and reduce waste. Residents are encouraged to participate by recycling household items and composting organic waste.', [(16, 1.4), (27, 1.6), (38, 2.1), (49, 2.6), (60, 3.1), (71, 3.6), (82, 4.1), (93, 4.6), (104, 5.1), (115, 5.6)]),
        (17, [17.0, 17.0, 17.0, 17.0, 17.0], 'A local theater group is performing a series of classic plays at the community center. The performances aim to make theater accessible to a wider audience.', [(17, 1.4), (28, 1.6), (39, 2.1), (50, 2.6), (61, 3.1), (72, 3.6), (83, 4.1), (94, 4.6), (105, 5.1), (116, 5.6)]),
        (18, [18.0, 18.0, 18.0, 18.0, 18.0], 'The city is investing in renewable energy projects, including the installation of solar panels on public buildings and the development of wind farms.', [(18, 1.4), (29, 1.6), (40, 2.1), (51, 2.6), (62, 3.1), (73, 3.6), (84, 4.1), (95, 4.6), (106, 5.1), (117, 5.6)]),
        (19, [19.0, 19.0, 19.0, 19.0, 19.0], 'A new sports complex is being built to provide facilities for basketball, soccer, and other sports. The complex will also include a fitness center and walking trails.', [(19, 1.4), (30, 1.6), (41, 2.1), (52, 2.6), (63, 3.1), (74, 3.6), (85, 4.1), (96, 4.6), (107, 5.1), (118, 5.6)]),
        (20, [20.0, 20.0, 20.0, 20.0, 20.0], 'The city is hosting a series of workshops on financial literacy, aimed at helping residents manage their money and plan for the future. Topics include budgeting, saving, and investing.', [(20, 1.4), (31, 1.6), (42, 2.1), (53, 2.6), (64, 3.1), (75, 3.6), (86, 4.1), (97, 4.6), (108, 5.1), (119, 5.6)]),
        (21, [21.0, 21.0, 21.0, 21.0, 21.0], 'A new art exhibit is opening at the city museum, featuring works by contemporary artists from around the world. The exhibit aims to foster a greater appreciation for modern art.', [(21, 1.4), (32, 1.6), (43, 2.1), (54, 2.6), (65, 3.1), (76, 3.6), (87, 4.1), (98, 4.6), (109, 5.1), (120, 5.6)]),
        (22, [22.0, 22.0, 22.0, 22.0, 22.0], 'The local animal shelter is holding an adoption event this weekend. Dogs, cats, and other pets are available for adoption, and volunteers will be on hand to provide information.', [(22, 1.4), (33, 1.6), (44, 2.1), (55, 2.6), (66, 3.1), (77, 3.6), (88, 4.1), (99, 4.6), (110, 5.1), (121, 5.6)]),
        (23, [23.0, 23.0, 23.0, 23.0, 23.0], 'The city is upgrading its water infrastructure to ensure a reliable supply of clean water. The project includes replacing old pipes and installing new treatment facilities.', [(23, 1.4), (34, 1.6), (45, 2.1), (56, 2.6), (67, 3.1), (78, 3.6), (89, 4.1), (100, 4.6), (111, 5.1), (122, 5.6)]),
        (24, [24.0, 24.0, 24.0, 24.0, 24.0], 'A new technology incubator has opened to support startups in the tech sector. The incubator provides office space, resources, and mentorship to help entrepreneurs succeed.', [(24, 1.4), (35, 1.6), (46, 2.1), (57, 2.6), (68, 3.1), (79, 3.6), (90, 4.1), (101, 4.6), (112, 5.1), (123, 5.6)]),
        (25, [25.0, 25.0, 25.0, 25.0, 25.0], 'The city is planning to build a new bike lane network to promote cycling as a healthy and environmentally friendly mode of transportation. The project includes dedicated bike lanes and bike-sharing stations.', [(25, 1.4), (36, 1.6), (47, 2.1), (58, 2.6), (69, 3.1), (80, 3.6), (91, 4.1), (102, 4.6), (113, 5.1), (124, 5.6)]),
        (26, [26.0, 26.0, 26.0, 26.0, 26.0], 'The local farmers market is reopening for the season, offering fresh produce, artisanal goods, and handmade crafts from local vendors.', [(26, 1.4), (37, 1.6), (48, 2.1), (59, 2.6), (70, 3.1), (81, 3.6), (92, 4.1), (103, 4.6), (114, 5.1), (125, 5.6)]),
        (27, [27.0, 27.0, 27.0, 27.0, 27.0], 'A new educational program is being launched to support early childhood development. The program provides resources and training for parents and caregivers.', [(27, 1.4), (38, 1.6), (49, 2.1), (60, 2.6), (71, 3.1), (82, 3.6), (93, 4.1), (104, 4.6), (115, 5.1), (126, 5.6)]),
        (28, [28.0, 28.0, 28.0, 28.0, 28.0], 'The city is organizing a series of concerts in the park, featuring performances by local bands and musicians. The concerts are free and open to the public.', [(28, 1.4), (39, 1.6), (50, 2.1), (61, 2.6), (72, 3.1), (83, 3.6), (94, 4.1), (105, 4.6), (116, 5.1), (127, 5.6)]),
        (29, [29.0, 29.0, 29.0, 29.0, 29.0], 'A new senior center is opening, offering programs and services for older adults. Activities include fitness classes, educational workshops, and social events.', [(29, 1.4), (40, 1.6), (51, 2.1), (62, 2.6), (73, 3.1), (84, 3.6), (95, 4.1), (106, 4.6), (117, 5.1), (128, 5.6)]),
        (30, [30.0, 30.0, 30.0, 30.0, 30.0], 'The city is implementing a new traffic management system to reduce congestion and improve safety. The system includes synchronized traffic lights and real-time traffic monitoring.', [(30, 1.4), (41, 1.6), (52, 2.1), (63, 2.6), (74, 3.1), (85, 3.6), (96, 4.1), (107, 4.6), (118, 5.1), (129, 5.6)]),
        (31, [31.0, 31.0, 31.0, 31.0, 31.0], 'A new community outreach program is being launched to support at-risk youth. The program provides mentoring, tutoring, and recreational activities.', [(31, 1.4), (42, 1.6), (53, 2.1), (64, 2.6), (75, 3.1), (86, 3.6), (97, 4.1), (108, 4.6), (119, 5.1), (130, 5.6)]),
        (32, [32.0, 32.0, 32.0, 32.0, 32.0], 'The city is hosting a series of public forums to discuss plans for future development. Residents are encouraged to attend and provide feedback on proposed projects.', [(32, 1.4), (43, 1.6), (54, 2.1), (65, 2.6), (76, 3.1), (87, 3.6), (98, 4.1), (109, 4.6), (120, 5.1), (131, 5.6)]),
        (33, [33.0, 33.0, 33.0, 33.0, 33.0], 'A new public art installation is being unveiled in the downtown area. The installation features sculptures and murals by local artists and aims to beautify the urban landscape.', [(33, 1.4), (44, 1.6), (55, 2.1), (66, 2.6), (77, 3.1), (88, 3.6), (99, 4.1), (110, 4.6), (121, 5.1), (132, 5.6)]),
        (34, [34.0, 34.0, 34.0, 34.0, 34.0], 'The local university is launching a new research center focused on sustainable development. The center will conduct research and provide education on environmental issues.', [(34, 1.4), (45, 1.6), (56, 2.1), (67, 2.6), (78, 3.1), (89, 3.6), (100, 4.1), (111, 4.6), (122, 5.1), (133, 5.6)]),
        (35, [35.0, 35.0, 35.0, 35.0, 35.0], 'The city is planning to expand its public Wi-Fi network to provide free internet access in parks, libraries, and other public spaces.', [(35, 1.4), (46, 1.6), (57, 2.1), (68, 2.6), (79, 3.1), (90, 3.6), (101, 4.1), (112, 4.6), (123, 5.1), (134, 5.6)]),
        (36, [36.0, 36.0, 36.0, 36.0, 36.0], 'A new community health clinic is opening, offering medical, dental, and mental health services. The clinic aims to provide affordable healthcare to underserved populations.', [(36, 1.4), (47, 1.6), (58, 2.1), (69, 2.6), (80, 3.1), (91, 3.6), (102, 4.1), (113, 4.6), (124, 5.1), (135, 5.6)]),
        (37, [37.0, 37.0, 37.0, 37.0, 37.0], 'The city is implementing a new emergency alert system to provide residents with real-time information during emergencies. The system includes mobile alerts and social media updates.', [(37, 1.4), (48, 1.6), (59, 2.1), (70, 2.6), (81, 3.1), (92, 3.6), (103, 4.1), (114, 4.6), (125, 5.1), (136, 5.6)]),
        (38, [38.0, 38.0, 38.0, 38.0, 38.0], 'A local nonprofit is organizing a job fair to connect job seekers with employers. The fair will feature workshops on resume writing, interview skills, and job search strategies.', [(38, 1.4), (49, 1.6), (60, 2.1), (71, 2.6), (82, 3.1), (93, 3.6), (104, 4.1), (115, 4.6), (126, 5.1), (137, 5.6)]),
        (39, [39.0, 39.0, 39.0, 39.0, 39.0], 'The city is hosting a series of environmental cleanup events, encouraging residents to participate in efforts to clean up parks, rivers, and other natural areas.', [(39, 1.4), (50, 1.6), (61, 2.1), (72, 2.6), (83, 3.1), (94, 3.6), (105, 4.1), (116, 4.6), (127, 5.1), (138, 5.6)]),
        (40, [40.0, 40.0, 40.0, 40.0, 40.0], 'A new fitness trail is being built in the local park, featuring exercise stations and informational signs to promote physical activity and wellness.', [(40, 1.4), (51, 1.6), (62, 2.1), (73, 2.6), (84, 3.1), (95, 3.6), (106, 4.1), (117, 4.6), (128, 5.1), (139, 5.6)]),
        (41, [41.0, 41.0, 41.0, 41.0, 41.0], 'The city is launching a new initiative to support small businesses, offering grants, training, and resources to help entrepreneurs grow their businesses.', [(41, 1.4), (52, 1.6), (63, 2.1), (74, 2.6), (85, 3.1), (96, 3.6), (107, 4.1), (118, 4.6), (129, 5.1), (140, 5.6)]),
        (42, [42.0, 42.0, 42.0, 42.0, 42.0], 'A new art school is opening, offering classes and workshops for aspiring artists of all ages. The school aims to foster creativity and provide a supportive environment for artistic development.', [(42, 1.4), (53, 1.6), (64, 2.1), (75, 2.6), (86, 3.1), (97, 3.6), (108, 4.1), (119, 4.6), (130, 5.1), (141, 5.6)]),
        (43, [43.0, 43.0, 43.0, 43.0, 43.0], 'The city is planning to improve its public transportation system by introducing electric buses and expanding routes to underserved areas.', [(43, 1.4), (54, 1.6), (65, 2.1), (76, 2.6), (87, 3.1), (98, 3.6), (109, 4.1), (120, 4.6), (131, 5.1), (142, 5.6)]),
        (44, [44.0, 44.0, 44.0, 44.0, 44.0], 'A new music festival is being organized to celebrate local talent and bring the community together. The festival will feature performances by local bands, food stalls, and family-friendly activities.', [(44, 1.4), (55, 1.6), (66, 2.1), (77, 2.6), (88, 3.1), (99, 3.6), (110, 4.1), (121, 4.6), (132, 5.1), (143, 5.6)]),
        (45, [45.0, 45.0, 45.0, 45.0, 45.0], 'The city is implementing new measures to protect green spaces, including the creation of new parks and the preservation of existing natural areas.', [(45, 1.4), (56, 1.6), (67, 2.1), (78, 2.6), (89, 3.1), (100, 3.6), (111, 4.1), (122, 4.6), (133, 5.1), (144, 5.6)]),
        (46, [46.0, 46.0, 46.0, 46.0, 46.0], 'A new housing project is being developed to provide affordable homes for low-income families. The project includes energy-efficient buildings and community amenities.', [(46, 1.4), (57, 1.6), (68, 2.1), (79, 2.6), (90, 3.1), (101, 3.6), (112, 4.1), (123, 4.6), (134, 5.1), (145, 5.6)]),
        (47, [47.0, 47.0, 47.0, 47.0, 47.0], 'The city is hosting a series of workshops on entrepreneurship, providing training and resources for aspiring business owners. Topics include business planning, marketing, and finance.', [(47, 1.4), (58, 1.6), (69, 2.1), (80, 2.6), (91, 3.1), (102, 3.6), (113, 4.1), (124, 4.6), (135, 5.1), (146, 5.6)]),
        (48, [48.0, 48.0, 48.0, 48.0, 48.0], 'A new public garden is being created to provide a space for residents to relax and enjoy nature. The garden will feature walking paths, benches, and a variety of plants.', [(48, 1.4), (59, 1.6), (70, 2.1), (81, 2.6), (92, 3.1), (103, 3.6), (114, 4.1), (125, 4.6), (136, 5.1), (147, 5.6)]),
        (49, [49.0, 49.0, 49.0, 49.0, 49.0], 'The city is launching a campaign to promote public health, encouraging residents to get vaccinated, exercise regularly, and eat a balanced diet.', [(49, 1.4), (60, 1.6), (71, 2.1), (82, 2.6), (93, 3.1), (104, 3.6), (115, 4.1), (126, 4.6), (137, 5.1), (148, 5.6)]),
        (50, [50.0, 50.0, 50.0, 50.0, 50.0], 'A new community theater is opening, offering performances, workshops, and classes for residents of all ages. The theater aims to make the performing arts accessible to everyone.', [(50, 1.4), (61, 1.6), (72, 2.1), (83, 2.6), (94, 3.1), (105, 3.6), (116, 4.1), (127, 4.6), (138, 5.1), (149, 5.6)])
]

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
                    text String,
                    data Map(UInt32, Float32),
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
                    text String,
                    data Map(UInt32, Float32),
                    CONSTRAINT check_length CHECK length(vector) = 5
                ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard_2/local_table', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

        node_s1_r1.query("CREATE TABLE distributed_table(id UInt32, vector Array(Float32), text String, data Map(UInt32, Float32), CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = Distributed(test_cluster, default, local_table, id);")
        node_s1_r1.query("INSERT INTO distributed_table (id, vector, text, data) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster MATERIALIZE INDEX fts_ind;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster ADD INDEX sparse_ind data TYPE sparse;")
        node_s1_r1.query("ALTER TABLE local_table ON CLUSTER test_cluster MATERIALIZE INDEX sparse_ind;")

        # Create a MergeTree table with the same data as baseline.
        node_s1_r1.query("CREATE TABLE test_table (id UInt32, vector Array(Float32), text String, data Map(UInt32, Float32), CONSTRAINT check_length CHECK length(vector) = 5) ENGINE = MergeTree ORDER BY id;")
        node_s1_r1.query("INSERT INTO test_table (id, vector, text, data) VALUES {}".format(", ".join(map(str, data))))
        node_s1_r1.query("ALTER TABLE test_table ADD INDEX fts_ind text TYPE fts;")
        node_s1_r1.query("ALTER TABLE test_table MATERIALIZE INDEX fts_ind;")
        node_s1_r1.query("ALTER TABLE test_table ADD INDEX sparse_ind data TYPE sparse;")
        node_s1_r1.query("ALTER TABLE test_table MATERIALIZE INDEX sparse_ind;")

        time.sleep(2)

        yield cluster

    finally:
        cluster.shutdown()

def test_distributed_hybrid_search(started_cluster):

    ## Test RSF fusion_type
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")

    ## Test RRF fusion_type
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RRF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'built') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RRF')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'built') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")

    ## Test enable_nlq and operator
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=false', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=OR')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")
    assert node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM test_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1") == node_s1_r1.query("SELECT id, HybridSearch('fusion_type=RSF', 'enable_nlq=true', 'operator=AND')(vector, text, [10.1, 20.3, 30.5, 40.7, 50.9], 'city AND new') AS score FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1")

def test_refine_hybrid_search(started_cluster):
    vector_search = {'query_column': 'vector', 'query_value': '[10.1, 20.3, 30.5, 40.7, 50.9]'}
    text_search = {'query_column': 'text', 'query_value': '\'network OR system\''}
    sparse_search = {'query_column': 'data', 'query_value': 'map(10, 1.12, 11, 1.23, 12, 1.34, 13, 1.45, 14, 1.56, 15, 1.67, 16, 1.78, 17, 1.89, 18, 1.90, 19, 2.01, 20, 2.12)'}

    hybrid_search = [vector_search, sparse_search, text_search]

    # Generate all possible pairs of hybrid_search
    test_hybrid_search = list(itertools.permutations(hybrid_search, 2))

    for search in test_hybrid_search:
        for fusion_type in ['RSF']:
            local_baseline = """
                SELECT id, HybridSearch('fusion_type={}')({}, {}, {}, {}) AS score 
                FROM test_table ORDER BY score DESC, id ASC LIMIT 5 
                SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1
            """.format(fusion_type, search[0]['query_column'], search[1]['query_column'], search[0]['query_value'], search[1]['query_value'])

            distributed_query = """
                SELECT id, HybridSearch('fusion_type={}')({}, {}, {}, {}) AS score 
                FROM distributed_table ORDER BY score DESC, id ASC LIMIT 5 
                SETTINGS dfs_query_then_fetch = 1, enable_brute_force_vector_search = 1
            """.format(fusion_type, search[0]['query_column'], search[1]['query_column'], search[0]['query_value'], search[1]['query_value'])

            assert node_s1_r1.query(local_baseline) == node_s1_r1.query(distributed_query)

