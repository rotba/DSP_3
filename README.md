# DSP_3
- id: fill
- username: rotba

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
 

