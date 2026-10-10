# jnr-jenner-analytics-fast-parallel-sort-sixgb-dataset-jenner-datasteps-and-duckdb
Jenner Analytics fast parallel sorts sixgb dataset jenner datasteps and duckdb
    /*---
    c:/utl/jnr-jenner-analytics-fast-parallel-sort-sixgb-dataset-jenner-datasteps-and-duckdb.sas;
    ---*/
    
    Jenner Analytics fast parallel sorts sixgb dataset jenner datasteps and duckdb
    
    see
    https://tinyurl.com/mr34frmf
    https://github.com/rogerjdeangelis/jnr-jenner-analytics-fast-parallel-sort-sixgb-dataset-jenner-datasteps-and-duckdb
    
     CONTENTS
    
       1 Using Jenner Analytic datasteps run using Python subprocesses,
         seems better than SAS systask and avoids SAS some sassystem limits?
    
         Eight parallel jobs (sort 100 million rows by testcode)
    
         All jobs completed in  64.85 seconds!
         Parallel execution saved approximately 442.76 seconds compared to sequential execution
    
         Parallel Jobs
         Job2: Completed with return code 0 (took 61.83 seconds)
         Job6: Completed with return code 0 (took 62.29 seconds)
         Job0: Completed with return code 0 (took 62.30 seconds)
         Job4: Completed with return code 0 (took 62.29 seconds)
         Job1: Completed with return code 0 (took 64.61 seconds)
         Job5: Completed with return code 0 (took 64.72 seconds)
         Job7: Completed with return code 0 (took 64.72 seconds)
         Job3: Completed with return code 0 (took 61.43 seconds)
    
       2 Duckdb (automatic internal coded parallelism?)
    
        Total Time: 11.61s
    
        SET threads = 16; /*--- automatically allows threads to run on either xeon, even though I have 64, 16 is fater?      ---*/
        SET memory_limit = '64GB'; /*--- faster tham 32 or 64 threads and 96gb or 128gb memory  Restricts socket switching?  ---*/
    
        DuckDB is usually much faster than PostgreSQL, SQLite, and MySQL at analytics on one machine, but it isn’t faster at everything.
    
    related
    https://github.com/rogerjdeangelis/utl-altair-slc-paralell-processing-of-almost-two-billion-obs-io-bound-and-compute-bound-examples
    
    SOAPBOX ON
    I think this is accurate?
    
    FYI: Jenner does not limit the number of CPU  sockets, cores or threads,pretty sure of this.
         DUCKDB seems optimized for parallel processing? Microsoft did not seem to limit systems with win 11, like it has done in the past?
         SAS charges extra based on CPU count and core count. I don't believe the SLC charges extra?
    
    SOAPBOX OFF
    
    This post used Win 11 Del T7610 Dual 3.3gz XEONS, 128gb Ram, 64 threads, 6 drives consisting of 4 hot swap SSDs
    and 2 internal NVMe 1tb Drives ($600 on Ebay) also 3 32inch monitors.
    
    PROBLEM: SORT 6GB 100 MILLION ROWS BY TESTCD
    
      INPUT FIRST 5 ROWS of input 100 million rows
    
    ┌─────────┬─────────┬─────────┬────────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │  obno   │    key     │ subject │ visit │ result │
    │ varchar │ varchar │  int64  │   int64    │  int64  │ int64 │ double │
    ├─────────┼─────────┼─────────┼────────────┼─────────┼───────┼────────┤
    │ GLUC    │ mg/dL   │ 7415297 │ 1461516085 │    4086 │     1 │    8.5 │
    │ BILI    │ mg/dL   │ 7415298 │ 1461564356 │    4357 │     7 │   35.6 │
    │ AST     │ mg/dL   │ 7415299 │ 1461612627 │    4628 │    13 │   62.7 │
    │ ALT     │ U/L     │ 7415300 │ 1461660898 │    4899 │     6 │   89.8 │
    │ A1C     │ U/L     │ 7415301 │ 1461709169 │    5170 │    12 │   16.9 │
    └─────────┴─────────┴─────────┴────────────┴─────────┴───────┴────────┘
    
    FIRST 5 AFTER SORT
    
    ┌─────────┬─────────┬──────────┬───────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │   obno   │    key    │ subject │ visit │ result │
    │ varchar │ varchar │  int64   │   int64   │  int64  │ int64 │ double │
    ├─────────┼─────────┼──────────┼───────────┼─────────┼───────┼────────┤
    │ A1C     │ U/L     │ 40992768 │ 929465241 │    1242 │     3 │   24.1 │
    │ A1C     │ U/L     │ 40992776 │ 929851409 │    3410 │    12 │   40.9 │
    │ A1C     │ U/L     │ 40992784 │ 930237577 │    5578 │     8 │   57.7 │
    │ A1C     │ U/L     │ 40992792 │ 930623745 │    7746 │     4 │   74.5 │
    │ A1C     │ U/L     │ 40992800 │ 931009913 │    1914 │     1 │   91.3 │
    └─────────┴─────────┴──────────┴───────────┴─────────┴───────┴────────┘
    
    
    /******************************************************************************************************************/
    /*  1 INPUT FOR EIGHT PARALLEL JOBS (SORT 100 MILLION ROWS BY TESTCODE)                                           */
    /******************************************************************************************************************/
    
    options obs=max;
    %let nsubj=8000;
    
    data "d:/csv/labds100e6.csv";
      length testcd $8 unit $8;
      array tc[8] $8 _temporary_ ("CREAT" "A1C" "ALT" "AST" "BILI" "GLUC" "HGB" 'PLAT' );
      array un[8] $8 _temporary_ ('10^9/L' 'U/L' 'U/L' 'mg/dL' 'mg/dL' 'mg/dL' 'mg/dL' 'mg/dL'  );
    
      do obno = 1 to 100000000;
        key     = mod(obno * 48271, 2147483647);
        subject = mod(key, &nsubj) + 1;
        visit   = mod(floor(key / &nsubj), 13) + 1;
        k       = mod(key, 8) + 1;
        testcd  = tc[k];
        unit    = un[k];
        result  = mod(key, 1000) / 10;
        output;
      end;
    
      stop;
      drop k;
    run;
    
    options obs=max;
    libname duckdb parquet "d:/duckdb";
    proc contents data= "d:/csv/labds100e6.csv" ;
    run;
    
    /******************************************************************************************************************/
    /*  1 LIST                                                                                                        */
    /******************************************************************************************************************/
    
    PROC CONTENTS
    -------------------------------------------------------------------------------
    
    Data Set Name: d:/csv/labds100e6.csv
    Observations:  100000000
    Variables:     7
    
    Variables (in alphabetical order):
    
      #  Variable  Type    Len  Format  Label
    ---  --------  ----  -----  ------  -----
      4  key        Num      8
      3  obno       Num      8
      7  result     Num      8
      5  subject    Num      8
      1  testcd    Char      8
      2  unit      Char      8
      6  visit      Num      8
    
    -------------------------------------------------------------------------------
    
    /******************************************************************************************************************/
    /*  1 CREATE INPUT AVRO FILE JENNER PARALLEL DATASTEPS                                                            */
    /******************************************************************************************************************/
    
    options obs=max;
    libname avro avro "d:/avro";
    data avro.inp;
        set "d:/csv/labds100e6.csv";
    run;
    
    /******************************************************************************************************************/
    /*  1 LOG CREATE INPUT AVRO FILE JENNER PARALLEL DATASTEPS                                                        */
    /******************************************************************************************************************/
    
    NOTE: Copyright (c) 2026 Jenner Analytics Ltd., London, England.
    NOTE: Jenner v1.5.92 (build v1.5.92+0624ca02cf.20261005T015448Z.x86_64-pc-windows-msvc)
          Licensed to Roger DeAngelis, Serial 3BF7B3E6.
    NOTE: DATA _null_
    
    autexec started.
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Library WORKX assigned path=d:\wpswrkx.
    NOTE: DATA workx.class
    
    NOTE: Processing inline DATALINES (19 lines)
    
    NOTE: Read 19 rows from DATALINES.
    NOTE: The data set WORKX.CLASS has 19 observations and 5 variables.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    LOG:   9:51:07
    NOTE: DATA _null_ completed. Output written to FILE PRINT
    NOTE: Option SASAUTOS changed to c:/otojnr.
    NOTE: DATA _null_
    
    NOTE: Reading from fileref c:/jnr/runsas_selection.sas (c:/jnr/runsas_selection.sas)
        1 options obs=max;
        2 libname avro avro "d:/avro";
        3 data avro.inp;
        4     set "d:/csv/labds100e6.csv";
        5 run;
    
    NOTE: Read 5 rows from c:/jnr/runsas_selection.sas.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.02 seconds
    NOTE: DATA _null_
    
    autoexec completed   9:51:07
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Option OBS changed to MAX.
    NOTE: Library AVRO assigned path=d:/avro.
    NOTE: DATA avro.inp
    
    NOTE: Read 100000000 rows from d:/csv/labds100e6.csv.
    NOTE: The data set AVRO.INP has 100000000 observations and 7 variables.
    NOTE: DATA elapsed:
      wall  4:32.57
      cpu   4:30.50
    
    /******************************************************************************************************************/
    /*  1 CREATE EIGHT DATASTEP JOBS FOR SIMULTANEOUS EXECUTION                                                       */
    /******************************************************************************************************************/
    
    data _null_;
     rc=dcreate('jnrc1','c:/');
     rc=dcreate('jnre1','e:/');
     rc=dcreate('jnrf1','f:/');
     rc=dcreate('jnrg1','g:/');
     rc=dcreate('jnrh1','h:/');
     rc=dcreate('jnrc2','c:/');
     rc=dcreate('jnre2','e:/');
     rc=dcreate('jnrf2','f:/');
    run;
    
    libname jnr0c avro "c:/jnrc1";
    libname jnr1e avro "e:/jnre1";
    libname jnr2f avro "f:/jnrf1";
    libname jnr3g avro "g:/jnrg1";
    libname jnr4h avro "h:/jnrh1";
    libname jnr5c avro "c:/jnrc2";
    libname jnr6e avro "e:/jnre2";
    libname jnr7f avro "f:/jnrf2";
    
    /*--- IN CASE YOU RERUN ---*/
    proc delete data= jnr0c.A1C ;run;
    proc delete data= jnr1e.ALT ;run;
    proc delete data= jnr2f.AST ;run;
    proc delete data= jnr3g.BILI ;run;
    proc delete data= jnr4h.CREAT ;run;
    proc delete data= jnr5c.GLUC ;run;
    proc delete data= jnr6e.HGB ;run;
    proc delete data= jnr7f.PLAT ;run;
    
    data _null_;
    
     array jobs[8] $144 (
       'libname jnr0c avro "c:/jnrc1";proc sort data =avro.inp (where=(testcd="A1C"))   out= jnr0c.A1C   sortsize=8g noequals;by testcd ;run;'
       'libname jnr1e avro "e:/jnre1";proc sort data =avro.inp (where=(testcd="ALT"))   out= jnr1e.ALT   sortsize=8g noequals;by testcd ;run;'
       'libname jnr2f avro "f:/jnrf1";proc sort data =avro.inp (where=(testcd="AST"))   out= jnr2f.AST   sortsize=8g noequals;by testcd ;run;'
       'libname jnr3g avro "g:/jnrg1";proc sort data =avro.inp (where=(testcd="BILI"))  out= jnr3g.BILI  sortsize=8g noequals;by testcd ;run;'
       'libname jnr4h avro "h:/jnrh1";proc sort data =avro.inp (where=(testcd="CREAT")) out= jnr4h.CREAT sortsize=8g noequals;by testcd ;run;'
       'libname jnr5c avro "c:/jnrc1";proc sort data =avro.inp (where=(testcd="GLUC"))  out= jnr5c.GLUC  sortsize=8g noequals;by testcd ;run;'
       'libname jnr6e avro "e:/jnre2";proc sort data =avro.inp (where=(testcd="HGB"))   out= jnr6e.HGB   sortsize=8g noequals;by testcd ;run;'
       'libname jnr7f avro "f:/jnrf2";proc sort data =avro.inp (where=(testcd="PLAT"))  out= jnr7f.PLAT  sortsize=8g noequals;by testcd ;run;'
       );
     array fyls[8] $16 (
        'd:/sas/j0c.sas'
        'd:/sas/j1e.sas'
        'd:/sas/j2f.sas'
        'd:/sas/j3g.sas'
        'd:/sas/j4h.sas'
        'd:/sas/j5c.sas'
        'd:/sas/j6e.sas'
        'd:/sas/j7f.sas'
        );
     do j=1 to 10;
       filen=fyls[j];
       file outfile filevar=filen;
       put 'libname avro avro "d:/avro";';
       put jobs[j];
     end;
    stop;
    run;quit;
    
    /*--- WHAT A TIPICAL JOB LOOKS LIKE
    libname avro avro "d:/avro";
    libname jnr0 avro "c:/jnrc1";proc sort data =avro.inp (where=(testcd="A1C"))   out= jnr0.A1C   sortsize=8g noequals;by testcd ;run;
    ---*/
    
    /******************************************************************************************************************/
    /*  1 RUN PYTHON PARALLELIZATION OF SAS JOBS                                                                      */
    /******************************************************************************************************************/
    
    options set=PYTHONHOME "D:\py314";
    proc python;
    submit;
    import subprocess
    import concurrent.futures
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os
    import time  # Add time module
    
    def run_wps_job(job_name, script_path, log_path):
        """Run a single WPS job"""
        wps_exe = r"jenner"
        cmd = [wps_exe, '-sysin', script_path, '-log', log_path]
    
        print(f"Starting {job_name}...")
    
        # Time the individual job
        job_start = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        job_end = time.time()
    
        job_duration = job_end - job_start
        print(f"{job_name}: Completed in {job_duration:.2f} seconds")
    
        # Check if log file was created
        if os.path.exists(log_path):
            print(f"{job_name}: Log file created at {log_path}")
    
        return job_name, result.returncode, job_duration
    
    # Define jobs
    jobs = [
        ("Job0", r"d:\sas\j0.sas", r"d:\log\j0.log"),
        ("Job1", r"d:\sas\j1.sas", r"d:\log\j1.log"),
        ("Job2", r"d:\sas\j2.sas", r"d:\log\j2.log"),
        ("Job3", r"d:\sas\j3.sas", r"d:\log\j3.log"),
        ("Job4", r"d:\sas\j4.sas", r"d:\log\j4.log"),
        ("Job5", r"d:\sas\j5.sas", r"d:\log\j5.log"),
        ("Job6", r"d:\sas\j6.sas", r"d:\log\j6.log"),
        ("Job7", r"d:\sas\j7.sas", r"d:\log\j7.log")
    ]
    
    # Time the entire parallel execution
    total_start = time.time()
    
    # Run jobs in parallel using ThreadPoolExecutor (works better within SAS)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all jobs
        futures = [executor.submit(run_wps_job, *job) for job in jobs]
    
        # Wait for all to complete and get results
        for future in concurrent.futures.as_completed(futures):
            job_name, returncode, job_duration = future.result()
            print(f"{job_name}: Completed with return code {returncode} (took {job_duration:.2f} seconds)")
    
    total_end = time.time()
    total_duration = total_end - total_start
    
    print(f"All jobs completed in  {total_duration:.2f} seconds!")
    print(f"Parallel execution saved approximately {sum([f.result()[2] for f in futures]) - total_duration:.2f} seconds compared to sequential execution")
    endsubmit;
    run;
    
    /******************************************************************************************************************/
    /*  1 LIST FOR  RUN PYTHON PARALLELIZATION OF SAS JOBS                                                            */
    /******************************************************************************************************************/
    
    Starting Job0...
    Starting Job1...
    Starting Job2...
    Starting Job3...
    Starting Job4...
    Starting Job5...
    Starting Job6...
    Starting Job7...
    Job2: Completed in 61.83 seconds
    Job2: Log file created at d:\log\j2.log
    Job2: Completed with return code 0 (took 61.83 seconds)
    Job6: Completed in 62.29 seconds
    Job6: Log file created at d:\log\j6.log
    Job6: Completed with return code 0 (took 62.29 seconds)
    Job0: Completed in 62.30 seconds
    Job0: Log file created at d:\log\j0.log
    Job0: Completed with return code 0 (took 62.30 seconds)
    Job4: Completed in 62.29 seconds
    Job4: Log file created at d:\log\j4.log
    Job4: Completed with return code 0 (took 62.29 seconds)
    Job1: Completed in 64.61 seconds
    Job1: Log file created at d:\log\j1.log
    Job1: Completed with return code 0 (took 64.61 seconds)
    Job5: Completed in 64.72 seconds
    Job7: Completed in 64.72 seconds
    Job5: Log file created at d:\log\j5.log
    Job7: Log file created at d:\log\j7.log
    Job5: Completed with return code 0 (took 64.72 seconds)
    Job7: Completed with return code 0 (took 64.72 seconds)
    Job3: Completed in 64.80 seconds
    Job3: Log file created at d:\log\j3.log
    Job3: Completed with return code 0 (took 64.80 seconds)
    All jobs completed in  64.81 seconds!
    Parallel execution saved approximately 442.76 seconds compared to sequential execution
    
    /******************************************************************************************************************/
    /*  1 LOG FOR  RUN PYTHON PARALLELIZATION OF SAS JOBS                                                             */
    /******************************************************************************************************************/
    
    NOTE: Copyright (c) 2026 Jenner Analytics Ltd., London, England.
    NOTE: Jenner v1.5.92 (build v1.5.92+0624ca02cf.20261005T015448Z.x86_64-pc-windows-msvc)
          Licensed to Roger DeAngelis, Serial 3BF7B3E6.
    NOTE: DATA _null_
    
    autexec started.
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Library WORKX assigned path=d:\wpswrkx.
    NOTE: DATA workx.class
    
    NOTE: Processing inline DATALINES (19 lines)
    
    NOTE: Read 19 rows from DATALINES.
    NOTE: The data set WORKX.CLASS has 19 observations and 5 variables.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    LOG:  10:14:04
    NOTE: DATA _null_ completed. Output written to FILE PRINT
    NOTE: Option SASAUTOS changed to c:/otojnr.
    NOTE: DATA _null_
    
    NOTE: Reading from fileref c:/jnr/runsas_selection.sas (c:/jnr/runsas_selection.sas)
        1 options set=PYTHONHOME "D:\py314";
        2 proc python;
        3 submit;
        4 import subprocess
        5 import concurrent.futures
        6 from concurrent.futures import ThreadPoolExecutor, as_completed
        7 import os
        8 import time  # Add time module
        9
       10 def run_wps_job(job_name, script_path, log_path):
       11     """Run a single WPS job"""
       12     wps_exe = r"jenner"
       13     cmd = [wps_exe, '-sysin', script_path, '-log', log_path]
       14
       15     print(f"Starting {job_name}...")
       16
       17     # Time the individual job
       18     job_start = time.time()
       19     result = subprocess.run(cmd, capture_output=True, text=True)
       20     job_end = time.time()
       21
       22     job_duration = job_end - job_start
       23     print(f"{job_name}: Completed in {job_duration:.2f} seconds")
       24
       25     # Check if log file was created
       26     if os.path.exists(log_path):
       27         print(f"{job_name}: Log file created at {log_path}")
       28
       29     return job_name, result.returncode, job_duration
       30
       31 # Define jobs
       32 jobs = [
       33     ("Job0", r"d:\sas\j0.sas", r"d:\log\j0.log"),
       34     ("Job1", r"d:\sas\j1.sas", r"d:\log\j1.log"),
       35     ("Job2", r"d:\sas\j2.sas", r"d:\log\j2.log"),
       36     ("Job3", r"d:\sas\j3.sas", r"d:\log\j3.log"),
       37     ("Job4", r"d:\sas\j4.sas", r"d:\log\j4.log"),
       38     ("Job5", r"d:\sas\j5.sas", r"d:\log\j5.log"),
       39     ("Job6", r"d:\sas\j6.sas", r"d:\log\j6.log"),
       40     ("Job7", r"d:\sas\j7.sas", r"d:\log\j7.log")
       41 ]
       42
       43 # Time the entire parallel execution
       44 total_start = time.time()
       45
       46 # Run jobs in parallel using ThreadPoolExecutor (works better within SAS)
       47 with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
       48     # Submit all jobs
       49     futures = [executor.submit(run_wps_job, *job) for job in jobs]
       50
       51     # Wait for all to complete and get results
       52     for future in concurrent.futures.as_completed(futures):
       53         job_name, returncode, job_duration = future.result()
       54         print(f"{job_name}: Completed with return code {returncode} (took {job_duration:.2f} seconds)")
       55
       56 total_end = time.time()
       57 total_duration = total_end - total_start
       58
       59 print(f"All jobs completed in  {total_duration:.2f} seconds!")
       60 print(f"Parallel execution saved approximately {sum([f.result()[2] for f in futures]) - total_duration:.2f} seconds compared to sequential execution")
       61 endsubmit;
       62 run;
    
    NOTE: Read 62 rows from c:/jnr/runsas_selection.sas.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    autoexec completed  10:14:04
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Option SET changed to PYTHONHOME D:\py314.
    NOTE: PROC PYTHON step executing.
    NOTE: PROC PYTHON step completed.
    
    /******************************************************************************************************************/
    /*  2 DUCKDB SORT SOLUTION                                                                                        */
    /******************************************************************************************************************/
    
    %let start=%sysfunc(time());
    %jnr_duckbeginx;
    cards4;
    SET threads = 16;
    SET memory_limit = '64GB';
    SET preserve_insertion_order = false;
    drop table if exists labs;
    drop table if exists labs_sort;
    EXPLAIN ANALYZE create table labs as select * from read_csv('d:/csv/labds100e6.csv');
    SET preserve_insertion_order = true;
    EXPLAIN ANALYZE create table labs_sort as select * from labs order by testcd;
    select * from labs limit 10;
    select * from labs_sort limit 10;
    select * from labs limit 10 offset (select count(*) - 10 from labs);
    select * from labs_sort limit 10 offset (select count(*) - 10 from labs);
    EXPLAIN ANALYZE COPY labs_sort TO 'd:/duckdb/labs_sort.parquet' (FORMAT PARQUET);
    ;;;;
    %jnr_duckendx(duckdb=d:\duckdb\mydb.duckdb);
    %put Wall Clock Seconds = %sysevalf(%sysfunc(time()) - &start);
    
    proc contents data='d:/duckdb/labs_sort.parquet';
    run;
    
    /******************************************************************************************************************/
    /*  2 DUCKDB LIST OUTPUT CONTENTS                                                                                */
    /******************************************************************************************************************/
    
    PROC CONTENTS
    -------------------------------------------------------------------------------
    
    Data Set Name: d:/duckdb/labs_sort.parquet
    Observations:  100000000
    Variables:     7
    
    Variables (in alphabetical order):
    
      #  Variable  Type    Len  Format  Label
    ---  --------  ----  -----  ------  -----
      4  key        Num      8
      3  obno       Num      8
      7  result     Num      8
      5  subject    Num      8
      1  testcd    Char      8
      2  unit      Char      8
      6  visit      Num      8
    
    -------------------------------------------------------------------------------
    
    /******************************************************************************************************************/
    /*  2 DUCKDB LOG                                                                                                  */
    /******************************************************************************************************************/
    
    NOTE: Copyright (c) 2026 Jenner Analytics Ltd., London, England.
    NOTE: Jenner v1.5.92 (build v1.5.92+0624ca02cf.20261005T015448Z.x86_64-pc-windows-msvc)
          Licensed to Roger DeAngelis, Serial 3BF7B3E6.
    NOTE: DATA _null_
    
    autexec started.
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Library WORKX assigned path=d:\wpswrkx.
    NOTE: DATA workx.class
    
    NOTE: Processing inline DATALINES (19 lines)
    
    NOTE: Read 19 rows from DATALINES.
    NOTE: The data set WORKX.CLASS has 19 observations and 5 variables.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.02 seconds
    NOTE: DATA _null_
    
    LOG:  13:41:16
    NOTE: DATA _null_ completed. Output written to FILE PRINT
    NOTE: Option SASAUTOS changed to c:/otojnr.
    NOTE: DATA _null_
    
    NOTE: Reading from fileref c:/jnr/runsas_selection.sas (c:/jnr/runsas_selection.sas)
        1 %let start=%sysfunc(time());
        2 %jnr_duckbeginx;
        3 cards4;
        4 SET threads = 16;
        5 SET memory_limit = '64GB';
        6 SET preserve_insertion_order = false;
        7 drop table if exists labs;
        8 drop table if exists labs_sort;
        9 EXPLAIN ANALYZE create table labs as select * from read_csv('d:/csv/labds100e6.csv');
       10 EXPLAIN ANALYZE create table labs_sort as select * from labs order by testcd;
       11 select * from labs limit 10;
       12 select * from labs_sort limit 10;
       13 select * from labs limit 10 offset (select count(*) - 10 from labs);
       14 select * from labs_sort limit 10 offset (select count(*) - 10 from labs);
       15 EXPLAIN ANALYZE COPY labs_sort TO 'd:/duckdb/labs_sort.parquet' (FORMAT PARQUET);
       16 ;;;;
       17 %jnr_duckendx(duckdb=d:\duckdb\mydb.duckdb);
       18 %put Wall Clock Seconds = %sysevalf(%sysfunc(time()) - &start);
    
    NOTE: Read 18 rows from c:/jnr/runsas_selection.sas.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    autoexec completed  13:41:16
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    NOTE: Writing to fileref D:\wpswrk/duck_resolve.sql (D:\wpswrk/duck_resolve.sql)
    NOTE: Processing inline DATALINES (12 lines)
    SET threads = 32;
    SET memory_limit = '96GB';
    SET preserve_insertion_order = false;
    drop table if exists labs;
    drop table if exists labs_sort;
    EXPLAIN ANALYZE create table labs as select * from read_csv('d:/csv/labds100e6.csv');
    SET preserve_insertion_order = true;
    EXPLAIN ANALYZE create table labs_sort as select * from labs order by testcd;
    select * from labs limit 10;
    select * from labs_sort limit 10;
    select * from labs limit 10 offset (select count(*) - 10 from labs);
    select * from labs_sort limit 10 offset (select count(*) - 10 from labs);
    EXPLAIN ANALYZE COPY labs_sort TO 'd:/duckdb/labs_sort.parquet' (FORMAT PARQUET);
    
    NOTE: Read 12 rows from DATALINES.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    Wall Clock Seconds = 0.08785999999963678
    NOTE: Fileref RUT assigned to device PIPE (payload: duckdb d:\duckdb\mydb.duckdb -f D:\wpswrk/duck_resolve.sql).
    NOTE: DATA _null_
    
    NOTE: Reading from fileref rut (D:\wpswrk\jenner_pipe_RUT_94dadca5298e4551a65f0d96165d8c68.tmp)
    ┌─────────────────────────────────────┐
    │┌───────────────────────────────────┐│
    ││    Query Profiling Information    ││
    │└───────────────────────────────────┘│
    └─────────────────────────────────────┘
    EXPLAIN ANALYZE create table labs as select * from read_csv('d:/csv/labds100e6.csv');
    ┌────────────────────────────────────────────────┐
    │┌──────────────────────────────────────────────┐│
    ││              Total Time: 11.49s              ││
    │└──────────────────────────────────────────────┘│
    └────────────────────────────────────────────────┘
    
    EXPLAIN ANALYZE create table labs_sort as select * from labs order by testcd;
    ┌────────────────────────────────────────────────┐
    │┌──────────────────────────────────────────────┐│
    ││              Total Time: 10.98s              ││
    │└──────────────────────────────────────────────┘│
    └────────────────────────────────────────────────┘
    
     FIRST 10 BEFORE SORT
    
    ┌─────────┬─────────┬──────────┬───────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │   obno   │    key    │ subject │ visit │ result │
    │ varchar │ varchar │  int64   │   int64   │  int64  │ int64 │ double │
    ├─────────┼─────────┼──────────┼───────────┼─────────┼───────┼────────┤
    │ GLUC    │ mg/dL   │ 16925615 │ 972575805 │    7806 │     9 │   80.5 │
    │ BILI    │ mg/dL   │ 16925616 │ 972624076 │      77 │     3 │    7.6 │
    │ AST     │ mg/dL   │ 16925617 │ 972672347 │     348 │     9 │   34.7 │
    │ ALT     │ U/L     │ 16925618 │ 972720618 │     619 │     2 │   61.8 │
    │ A1C     │ U/L     │ 16925619 │ 972768889 │     890 │     8 │   88.9 │
    │ CREAT   │ 10^9/L  │ 16925620 │ 972817160 │    1161 │     1 │   16.0 │
    │ PLAT    │ mg/dL   │ 16925621 │ 972865431 │    1432 │     7 │   43.1 │
    │ HGB     │ mg/dL   │ 16925622 │ 972913702 │    1703 │    13 │   70.2 │
    │ GLUC    │ mg/dL   │ 16925623 │ 972961973 │    1974 │     6 │   97.3 │
    │ BILI    │ mg/dL   │ 16925624 │ 973010244 │    2245 │    12 │   24.4 │
    └─────────┴─────────┴──────────┴───────────┴─────────┴───────┴────────┘
    
     FIRST 10 AFTER SORT                                         7 columns
    ┌─────────┬─────────┬──────────┬────────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │   obno   │    key     │ subject │ visit │ result │
    │ varchar │ varchar │  int64   │   int64    │  int64  │ int64 │ double │
    ├─────────┼─────────┼──────────┼────────────┼─────────┼───────┼────────┤
    │ A1C     │ U/L     │ 76363371 │ 1054343289 │    7290 │    12 │   28.9 │
    │ A1C     │ U/L     │ 61189646 │  895387441 │    3442 │     7 │   44.1 │
    │ A1C     │ U/L     │ 54382477 │  871530633 │    2634 │     2 │   63.3 │
    │ A1C     │ U/L     │ 77535373 │ 1793477009 │    5010 │    13 │    0.9 │
    │ A1C     │ U/L     │ 19246223 │ 1321494929 │    6930 │     9 │   92.9 │
    │ A1C     │ U/L     │ 15583661 │  619623681 │    7682 │    12 │   68.1 │
    │ A1C     │ U/L     │ 25908069 │  772916145 │    4146 │    12 │   14.5 │
    │ A1C     │ U/L     │ 59402766 │  540248841 │     842 │    10 │   84.1 │
    │ A1C     │ U/L     │ 78436210 │  180623249 │    7250 │    10 │   24.9 │
    │ A1C     │ U/L     │ 96822687 │  803508305 │    4306 │     1 │   30.5 │
    └─────────┴─────────┴──────────┴────────────┴─────────┴───────┴────────┘
    
     LAST 10 BEFORE SORT                                         7 columns
    ┌─────────┬─────────┬──────────┬────────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │   obno   │    key     │ subject │ visit │ result │
    │ varchar │ varchar │  int64   │   int64    │  int64  │ int64 │ double │
    ├─────────┼─────────┼──────────┼────────────┼─────────┼───────┼────────┤
    │ AST     │ mg/dL   │ 99727414 │ 1431148267 │    4268 │     1 │   26.7 │
    │ ALT     │ U/L     │ 99727415 │ 1431196538 │    4539 │     7 │   53.8 │
    │ A1C     │ U/L     │ 99727416 │ 1431244809 │    4810 │    13 │   80.9 │
    │ CREAT   │ 10^9/L  │ 99727417 │ 1431293080 │    5081 │     6 │    8.0 │
    │ PLAT    │ mg/dL   │ 99727418 │ 1431341351 │    5352 │    12 │   35.1 │
    │ HGB     │ mg/dL   │ 99727419 │ 1431389622 │    5623 │     5 │   62.2 │
    │ GLUC    │ mg/dL   │ 99727420 │ 1431437893 │    5894 │    11 │   89.3 │
    │ BILI    │ mg/dL   │ 99727421 │ 1431486164 │    6165 │     4 │   16.4 │
    │ AST     │ mg/dL   │ 99727422 │ 1431534435 │    6436 │    10 │   43.5 │
    │ ALT     │ U/L     │ 99727423 │ 1431582706 │    6707 │     3 │   70.6 │
    └─────────┴─────────┴──────────┴────────────┴─────────┴───────┴────────┘
    
    
     LAST 10 AFTER SORT                                             7 columns
    ┌─────────┬─────────┬──────────┬────────────┬─────────┬───────┬────────┐
    │ testcd  │  unit   │   obno   │    key     │ subject │ visit │ result │
    │ varchar │ varchar │  int64   │   int64    │  int64  │ int64 │ double │
    ├─────────┼─────────┼──────────┼────────────┼─────────┼───────┼────────┤
    │ PLAT    │ mg/dL   │ 40647778 │ 1456322127 │    2128 │     2 │   12.7 │
    │ PLAT    │ mg/dL   │ 70720782 │ 1411352839 │     840 │    10 │   83.9 │
    │ PLAT    │ mg/dL   │ 61705652 │   33709303 │    5304 │     2 │   30.3 │
    │ PLAT    │ mg/dL   │ 84885661 │  116943655 │    7656 │     6 │   65.5 │
    │ PLAT    │ mg/dL   │ 86162401 │ 1616918079 │    6080 │     4 │    7.9 │
    │ PLAT    │ mg/dL   │ 28773159 │ 1634722127 │    2128 │     7 │   12.7 │
    │ PLAT    │ mg/dL   │  8789830 │ 1239605471 │    5472 │     4 │   47.1 │
    │ PLAT    │ mg/dL   │ 28510801 │ 1855340991 │    4992 │    11 │   99.1 │
    │ PLAT    │ mg/dL   │  3663379 │  741308655 │    4656 │    13 │   65.5 │
    │ PLAT    │ mg/dL   │ 18041126 │ 1132316111 │    4112 │     9 │   11.1 │
    └─────────┴─────────┴──────────┴────────────┴─────────┴───────┴────────┘
    
    EXPLAIN ANALYZE COPY labs_sort TO 'd:/duckdb/labs_sort.parquet' (FORMAT PARQUET);
    ┌────────────────────────────────────────────────┐
    │┌──────────────────────────────────────────────┐│
    ││               Total Time: 8.29s              ││
    │└──────────────────────────────────────────────┘│
    └────────────────────────────────────────────────┘
    
    NOTE: Read 268 rows from rut.
    NOTE: DATA elapsed:
      wall  0.01 seconds
      cpu   0.00 seconds
    NOTE: Fileref RUT cleared.
    NOTE: PROC CONTENTS data=d:/duckdb/labs_sort.parquet
    
    NOTE: PROC CONTENTS completed: 100000000 observations, 7 variables
    
    /******************************************************************************************************************/
    /*  2 CHECK THE DUCKDB OUTPUT                                                                                     */
    /******************************************************************************************************************/
    
    libname avro avro  "d:/avro";
    
    data avro.check;
      set 'd:/duckdb/labs_sort.parquet';
        by testcd;
        if first.testcd then output;
    run;
    
    libname avro avro  "d:/avro";
    proc print data=avro.check;
    run;
    
    /******************************************************************************************************************/
    /*  2 LIST CHECK THE DUCKDB OUTPUT                                                                                */
    /******************************************************************************************************************/
    
      FIRST OBSERVATION FOR EACH TESTCODE
      Obs  testcd  unit    obno     key  subject  visit  result
    
        1  A1C     U/L        7  337897     1898      4    89.7
        2  ALT     U/L        6  289626     1627     11    62.6
        3  AST     mg/dL      5  241355     1356      5    35.5
        4  BILI    mg/dL      4  193084     1085     12     8.4
        5  CREAT   10^9/L     8  386168     2169     10    16.8
        6  GLUC    mg/dL      3  144813      814      6    81.3
        7  HGB     mg/dL      2   96542      543     13    54.2
        8  PLAT    mg/dL      1   48271      272      7    27.1
    
    /******************************************************************************************************************/
    /*  2 LOG  CHECK THE DUCKDB OUTPUT                                                                                */
    /******************************************************************************************************************/
    
    NOTE: Copyright (c) 2026 Jenner Analytics Ltd., London, England.
    NOTE: Jenner v1.5.92 (build v1.5.92+0624ca02cf.20261005T015448Z.x86_64-pc-windows-msvc)
          Licensed to Roger DeAngelis, Serial 3BF7B3E6.
    NOTE: DATA _null_
    
    autexec started.
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Library WORKX assigned path=d:\wpswrkx.
    NOTE: DATA workx.class
    
    NOTE: Processing inline DATALINES (19 lines)
    
    NOTE: Read 19 rows from DATALINES.
    NOTE: The data set WORKX.CLASS has 19 observations and 5 variables.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    LOG:  15:13:45
    NOTE: DATA _null_ completed. Output written to FILE PRINT
    NOTE: Option SASAUTOS changed to c:/otojnr.
    NOTE: DATA _null_
    
    NOTE: Reading from fileref c:/jnr/runsas_selection.sas (c:/jnr/runsas_selection.sas)
        1 libname avro avro  "d:/avro";
        2
        3 data avro.check;
        4   set 'd:/duckdb/labs_sort.parquet';
        5     by testcd;
        6     if first.testcd then output;
        7 run;
        8
        9 libname avro avro  "d:/avro";
       10 proc print data=avro.check;
       11 run;
    
    NOTE: Read 11 rows from c:/jnr/runsas_selection.sas.
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: DATA _null_
    
    autoexec completed  15:13:45
    
    NOTE: DATA elapsed:
      wall  0.00 seconds
      cpu   0.00 seconds
    NOTE: Library AVRO assigned path=d:/avro.
    NOTE: DATA avro.check
    
    /******************************************************************************************************************/
    /*  EMD ALL                                                                                                       */
    /******************************************************************************************************************/
    
