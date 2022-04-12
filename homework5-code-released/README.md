# Homework 5

We strongly recommend that you use Visual Studio Code to do this homework.


## [50 points] Relational Algebra

We will explore some operators from relational algebra in the context of streaming data. The programming model of ToyDSL is a natural fit for this setting.

Fill in the code for the package `ra`. There are several `TODO` placeholders.

We have provided the class `UTestRA` to help you with testing.

1. Implement the streaming "group by" operator in `GroupBy.java`.

2. Implement the streaming theta join operator in `ThetaJoin.java`.

3. Implement the streaming equi-join operator in `EquiJoin.java`.

4. Execute the main method of `RelationalAlgebra.java`.


## [130 points] ECG Analysis

Use ToyDSL for the analysis of ECG signal.

The sampling frequency for `100.csv` is 360 Hz. This record has been obtained from the MIT-BIH dataset.

Fill in the code for the package `ecg`. There are several `TODO` placeholders.

We have provided the class `UTestECG` to help you with testing.

1. Start with the file `Data.java`. Execute the main method to print ECG data to the console.

2. Continue to implement the curve length transformation in `PeakDetection.java`.

3. The next step is to implement the detection algorithm (decision rule) in the file `Detect.java`.

4. Complete the `PeakDetection.java` file and run the main method to see the peak locations in the signal.

5. Continue with the analysis of the patient's heart rate in `HeartRate.java`.
