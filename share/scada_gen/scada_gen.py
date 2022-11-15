"""
Created on Oct. 25, 2022

@author: Inwoo Chung (gutomitai@cosw.space)
"""

import os
import glob
import sys

from scada_gen.celery import app


@app.task
def gen_scada(group_num, working_dir, wind_ts_filenames):
    '''Generate SCADA data using OpenFAST with wind vanes. '''
    
    # Create input files and relevant data files, and configure input files for openfast.
    for wind_ts_filename in wind_ts_filenames:
        # Create turbsim input files.
        ret = os.system(f"sed 's/sample.TimeSer/{wind_ts_filename}/' " + \
                        f"{working_dir}/10MWRWT/Flatirons_M2_turb_template.inp > " + \
                        f"{working_dir}/10MWRWT/{wind_ts_filename.split('.')[0] + '.inp'}") 
        if ret != 0:
            return 'Can\'t create turbsim input files.'
        
        # Generate data files for inflow.
        ret = os.system('docker run --rm -t ' + \
                          f'--user="defi" -v {working_dir}:/home/defi/dtu-10mw-rwt ' + \
                          '-w /home/defi/dtu-10mw-rwt -e "LD_LIBRARY_PATH=/usr/local/lib" ' + \
                          f"172.30.1.20/openfast-wind-sim:latest turbsim ./10MWRWT/{wind_ts_filename.split('.')[0] + '.inp'}")
        if ret != 0:
            return 'Can\'t create data files for inflow.'
        
        # Create fast files and inflow input files.
        ret = os.system(f"sed 's/sample.dat/DTU_10MW_InflowWind_{wind_ts_filename.split('.')[0]}.dat/' " + \
                  f"{working_dir}/DTU_10MW_RWT_Flatirons_M2_template.fst > " + \
                  f"{working_dir}/DTU_10MW_RWT_{wind_ts_filename.split('.')[0]}.fst")
        if ret != 0:
            return 'Can\'t create fast files.'
        
        ret = os.system(f"sed 's/sample.bts/{wind_ts_filename.split('.')[0]}.bts/' " + \
                        f"{working_dir}/10MWRWT/DTU_10MW_InflowWind_template.dat > " + \
                        f"{working_dir}/10MWRWT/DTU_10MW_InflowWind_{wind_ts_filename.split('.')[0]}.dat")
        if ret != 0:
            return 'Can\'t create inflow input files.'
        
    # Execute multiple openfast simulation tasks on one docker container.
    wind_ts_filename = wind_ts_filenames[0]
    print(group_num, wind_ts_filename)
    
    ret = os.system('docker run --rm -d ' + \
          f'--user="defi" -v {working_dir}:/home/defi/dtu-10mw-rwt ' + \
          '-w /home/defi/dtu-10mw-rwt -e "LD_LIBRARY_PATH=/usr/local/lib" ' + \
          f"--name wind-sim-{group_num} 172.30.1.20/openfast-wind-sim:latest openfast DTU_10MW_RWT_{wind_ts_filename.split('.')[0]}.fst")
    if ret != 0:
        return 'Can\'t simulate a wind turbine via openfast.'
    
    for wind_ts_filename in wind_ts_filenames[1:]:
        print(group_num, wind_ts_filename)
        ret = os.system('docker exec -d ' + \
              f"wind-sim-{group_num} openfast DTU_10MW_RWT_{wind_ts_filename.split('.')[0]}.fst")
        if ret != 0:
            return 'Can\'t simulate a wind turbine via openfast.'
    
    return "Succeed."
            
        
def operate_scada_gen(working_dir, num_group_members):
    '''Operate scanda data generation.'''
    
    # Get wind time series' file names.
    files = glob.glob(os.path.join(working_dir, '10MWRWT', '*.TimeSer'))
    files.remove('/home/nausicaa/workspace/dtu-10mw-rwt_with_baram/aeroelastic_models/fast/DTU10MWRWT_FAST_v1.00/ReferenceModal/10MWRWT/Flatirons_M2_20200508.TimeSer')
    files.remove('/home/nausicaa/workspace/dtu-10mw-rwt_with_baram/aeroelastic_models/fast/DTU10MWRWT_FAST_v1.00/ReferenceModal/10MWRWT/20140113_164000_inertial_MDR.TimeSer')
    files.sort()
    files = [v.split('/')[-1] for v in files]
    
    num_groups = len(files) // num_group_members
    remaining_num = len(files) % num_group_members
    
    print(num_groups, remaining_num)
    
    if remaining_num == 0:
        for group_num in range(3, 6):
            print(f'Group number: {group_num}')
            gen_scada(group_num, working_dir, files[(num_group_members * group_num):(num_group_members * (group_num + 1))])
    else:
        for group_num in range(3, 6):
            print(f'Group number: {group_num}')
            gen_scada(group_num, working_dir, files[(num_group_members * group_num):(num_group_members * (group_num + 1))])
        
        #print(f'Group number: {num_groups + 1}')
        #gen_scada(num_groups, working_dir, files[(num_group_members * num_groups):])
        

def main():
    working_dir = '/home/nausicaa/workspace/dtu-10mw-rwt_with_baram/aeroelastic_models/fast/DTU10MWRWT_FAST_v1.00/ReferenceModal'
    num_group_members = 5
    
    operate_scada_gen(working_dir, num_group_members)
    
if __name__ == '__main__':
    main()
    
    
    
            
