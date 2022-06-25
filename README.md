# DSP_3
- id: fill
- username: rotba

## Artifacts
- [F1-Measure]()
- [Precision-Recall Curve](https://s3.console.aws.amazon.com/s3/buckets/aws-logs-494081938343-us-east-1?region=us-east-1&prefix=elasticmapreduce/j-1DFJDUJD78PTU/steps/&showversions=false)
- Error analysis
  - Small input:
    - true-positive:
    - true-negative:
    - false-positive:
    - false-negative:
  - Large input:
    - true-positive:
    - true-negative:
    - false-positive:
    - false-negative:    
 
## System design
 ### Map reduce steps:
 - Terms: 
   - wN: |*,slot,w|
   - pN: |p,slot,N|
   - slotN: |\*,slot,\*|
 #### Calculation of all the wN's, pN's and of slotN and joining of wN:
 - (biarcs-data, positive-test-set, negative-test-set) -> STEP1_OUTPUT: Paths * Words * Slot -> {-1} * {-1} * N * N
 - Notes
   - Keys with a Path that is not associated to the test set will be emitted from the mapper only for the calculation of wN and slotN  
 #### Joing of pN and calculation of mi
 - STEP1_OUTPUT -> STEP2_OUTPUT: Paths * Words * Slot -> R
 #### Calculation of sim
 - (STEP2_OUTPUT,positive-test-set, negative-test-set) -> FINAL_OUTPUT: Path * Path -> R
 

