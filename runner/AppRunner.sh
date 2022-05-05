#!/bin/bash
python ../src/AppRunner.py /home/satishmekkonda/main/CobaGcpPoc/ GENERATE_DISTCP
python ../src/AppRunner.py /home/satishmekkonda/main/CobaGcpPoc/ EXECUTE_DISTCP
python ../src/AppRunner.py /home/satishmekkonda/main/CobaGcpPoc/ GENERATE_BQLOAD
python ../src/AppRunner.py /home/satishmekkonda/main/CobaGcpPoc/ EXECUTE_BQLOAD
python ../src/AppRunner.py /home/satishmekkonda/main/CobaGcpPoc/ VALIDATION_REPORT
#python ../src/AppRunner.py CREATE_VIEW
 
