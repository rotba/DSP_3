# DSP_3
- id: fill
- username: rotba
## Links
- [10 files from milion - mi](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/simj-162GXQ6452H1S/&showversions=false)
- [10 files from milion - sim](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/simj-162GXQ6452H1S/&showversions=false)
- [20 files from milion - mi](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/mij-26EQ0S9FMEEY9/&showversions=false)
- [20 files from milion - sim](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/simj-26EQ0S9FMEEY9/&showversions=false)
- [6 files from all - mi](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/mij-2KW18I2YFA113/&showversions=false)
- [6 files from all - sim](https://s3.console.aws.amazon.com/s3/buckets/dsp3?region=us-east-1&prefix=out/simj-2KW18I2YFA113/&showversions=false)

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
 

