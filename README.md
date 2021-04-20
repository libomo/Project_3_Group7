### CSCI3390 Large Scale Data Processing  &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; (Jien Li, Xinyu Yao, Mokun Li)

#### 1

| Graph file              | MIS file                     | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | ?Yes       |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | ?No        |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | ?No        |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | ?Yes       |



#### 2

| Graph file              | Iterations | Running Time/s | Verify |
| ----------------------- | ---------- | -------------- | ------ |
| small_edges.csv         | 2          | 1              | Yes    |
| line_100_edges.csv      | 1          | 1              | Yes    |
| twitter_100_edges.csv   | 2          | 1              | Yes    |
| twitter_1000_edges.csv  | 2          | 1              | Yes    |
| twitter_10000_edges.csv | 3          | 3              | Yes    |

#### 3

group7-3x4cores

21/04/20 02:57:47 WARN org.apache.spark.SparkContext: Using an existing SparkContext; some configuration may not take effect.

************************************************************
Current Iteration = 1. Remaining vertices = 6576890
************************************************************
************************************************************
Current Iteration = 2. Remaining vertices = 34184
************************************************************
************************************************************
Current Iteration = 3. Remaining vertices = 388
************************************************************
************************************************************
Current Iteration = 4. Remaining vertices = 1
************************************************************
************************************************************
Current Iteration = 5. Remaining vertices = 0
************************************************************
************************************************************
Total number of Iteration = 5
************************************************************
==================================

Luby's algorithm completed in 809s.

14 min 26 sec





group7-4x2cores

21/04/20 03:29:29 WARN org.apache.spark.SparkContext: Using an existing SparkContext; some configuration may not take effect.
************************************************************
Current Iteration = 1. Remaining vertices = 6737561
************************************************************
************************************************************
Current Iteration = 2. Remaining vertices = 39226
************************************************************
************************************************************
Current Iteration = 3. Remaining vertices = 558
************************************************************
************************************************************
Current Iteration = 4. Remaining vertices = 4
************************************************************
************************************************************
Current Iteration = 5. Remaining vertices = 0
************************************************************
************************************************************
Total number of Iteration = 5
************************************************************
==================================

Luby's algorithm completed in 1243s.

21 min 37 sec



