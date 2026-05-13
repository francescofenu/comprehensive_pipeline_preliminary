from airflow.operators.python import ExternalPythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow import DAG
import sys

EXTERNAL_PYTHON = "/home/gamma/envs/cosipy_laura/bin/python"

cosipy_yaml_input_file = "/home/gamma/workspace/data/transient/pipeline_zero4_galactic_large_2.yaml"
pipeline_configs= "/home/gamma/workspace/data/transient/config_pipeline2.txt"
base_funct_dir="/home/gamma/airflow/modules_pool/comprehensive-transient-analysis-pipeline/" # directory where common/ is located

sys.path.append(base_funct_dir)
from common.funzioni_comuni import read_base_pipeline_params
t_1,t_2,t_11,t_22,t_delta,cont_trigg,dir_out = read_base_pipeline_params(pipeline_configs)

def cleanup_and_format():
    # some cleanup and some service directory creation
    import subprocess
    subprocess.run('mkdir /home/gamma/workspace/data/transient/timescan', shell=True)
    subprocess.run('mkdir /home/gamma/workspace/data/garbagebin', shell=True)
    subprocess.run('mv /home/gamma/workspace/data/transient/tsel_* /home/gamma/workspace/data/garbagebin', shell=True)
    subprocess.run('mv /home/gamma/workspace/data/transient/bin* /home/gamma/workspace/data/garbagebin', shell=True)


def execute_bindata_bck_model(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)

    args=['--config',cosipy_yaml_input,'--config_group','bindata_bk','--overwrite', '--suffix','Background_Model','--output-dir',directory_output,'--tmin', str(t_scan_start_back), '--tmax', str(t_scan_stop_back)]
    cosi_bindata (argv=args)

def execute_bindata_grb(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params
    
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    args=['--config',cosipy_yaml_input,'--config_group','bindata_soubk','--overwrite', '--suffix','galbk_grbdc3','--output-dir',directory_output,'--tmin', str(t_scan_start_source), '--tmax', str(t_scan_stop_source)]
    cosi_bindata (argv=args)
    
def execute_tsmap_externaltrigger(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    import subprocess

    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params,read_trigger_content

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    outputFile=directory_output+'cosi-tsdetect_' + str(externalTrigger_start) + '.txt'
    newpngFileName=directory_output+'raw_ts_' + str(externalTrigger_start) + '.png'
    
    args=['--config',cosipy_yaml_input,'--output-dir',directory_output,'--overwrite','--tstart', str(externalTrigger_start), '--tstop', str(externalTrigger_stop)]
    with open(outputFile, "w") as f:
        with redirect_stdout(f):
            cosi_tsdetect (argv=args)
            subprocess.run('mv '+directory_output+'raw_ts.png '+newpngFileName, shell=True)
            
def execute_tsmap_scan(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    import subprocess
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params
    
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    subprocess.run('mkdir '+directory_output+'timescan', shell=True)

    fileNum=0
    print('|||||||||||||||||||||||||||||||||||||||||||||||',t_scan_start_source,t_scan_stop_source,t_scan_delta)
    for time in range(t_scan_start_source,t_scan_stop_source,t_scan_delta):
        outputFile=directory_output+'timescan/cosi-tsdetect_' + str(time) + '.txt'
        newpngFileName=directory_output+'timescan/raw_ts_' + str(time) + '.png'
        tstart=time
        tstop=time+t_scan_delta
        print('################ ',time)
        args=['--config',cosipy_yaml_input,'--output-dir',directory_output+'timescan','--overwrite','--tstart', str(tstart), '--tstop', str(tstop)]
        with open(outputFile, "w") as f:
            with redirect_stdout(f):
                print('cosi_tsdetect begin')
                cosi_tsdetect (argv=args)
                print('cosi_tsdetect end')
                subprocess.run('mv '+directory_output+'timescan/raw_ts.png '+newpngFileName, shell=True)
        fileNum+=1

def AnomalyDetection_autoencoder(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    import torch
    import subprocess
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params,read_file_histo
    from histpy import Histogram
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    
    #data_full     = Histogram.open("/home/gamma/workspace/data/transient/tsel_binned_data_galactic_Background_forTraining_BACKONLY.hdf5")
    timeBin = 1
    t0_test=12000
    t0_dataset=t0_test
    durationTot=1000
    durationEvent = 100
    numberEventstest = 10
    binNumTime=durationEvent/timeBin
    resolutionImage = 50
    FileName="/home/gamma/workspace/data/transient/FileOut_test_back_full_backgr"
    startPoint=t0_dataset
    endPoint=t0_dataset+durationTot
    
    #imagePlotX_Z_t_test = read_file_histo(t0_dataset,t0_dataset+durationTot,resolutionImage,binNumTime,FileName,durationEvent,numberEventstest)
    '''
    imagePlotX_Z_t_tmp = torch.zeros(numberEventstest,resolutionImage,resolutionImage,resolutionImage,int(binNumTime))
    values = data_full.slice[{ "Time": slice(startPoint,endPoint) }].project("PsiChi","Phi","Time").contents
    histoPhi = data_full.slice[{ "Time": slice(startPoint,endPoint) }].project('Phi')
    histoTime = data_full.slice[{ "Time": slice(startPoint,endPoint) }].project('Time')
    
    f_out = open(FileName,"w")
    referenceBin=0
    for iii in range(histoTime.nbins):
        if iii%eventDuration==0:
            referenceBin=iii
        binTime = int(histoTime.axes['Time'].centers[iii].value - histoTime.axes['Time'].centers[referenceBin].value)             


        for i in range(data_full.project('PsiChi').nbins):
            Psi = hp.pix2ang(8, i)[0]
            Chi = hp.pix2ang(8, i)[1]
            binPsi = int(np.rad2deg(Psi) / 180. * resolutionImage)
            binChi = int(np.rad2deg(Chi) / 360. * resolutionImage)

            for ii in range(histoPhi.nbins):
                binPhi = int(histoPhi.axes['Phi'].centers[ii].value / 180. * resolutionImage)
                if values[i][ii][iii]>0:
                    imagePlotX_Z_t_tmp[int(iii/eventDuration)][binPsi][binChi][binPhi][binTime] += values[i][ii][iii]
                    print(int(iii/eventDuration),binPsi,binChi,binPhi,binTime,values[i][ii][iii],file=f_out)
        if iii%10==0:
            print(float(iii)/float(histoTime.nbins)*100,' % of data processed ')
    f_out.close()
    '''
    
    from common.models import SimpleCNN3
    model = SimpleCNN3()
    state = torch.load('/home/gamma/workspace/data/transient/Model_AnomalyDetection.pth')
    model.load_state_dict(state)
    '''
    fileNum=0
    for time in range(t_scan_start,t_scan_stop):
        outputFile='/home/gamma/workspace/data/transient/timescan/cosi-AD_detect_' + str(fileNum) + '.txt'
    '''

        
def execute_threemlfit(cosipy_yaml_input,pipeline_input_file,outputdir,fitmodel,scanvar,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_threemlfit
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_cosi_ts_detect,read_base_pipeline_params,format_override_val,read_trigger_content
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    
    measured_l=float(0.)
    measured_b=float(0.)
    error_coo=float(0.)
    maxumumTS=float(0.)

    var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,modelname = format_override_val(fitmodel,measured_l,measured_b,error_coo)

    t_init=t_scan_start_source
    t_end=t_scan_stop_source
    t_step=t_scan_delta
    if scanvar==0:
        t_init=externalTrigger_start
        t_end=externalTrigger_stop
        t_step=externalTrigger_stop-externalTrigger_start
        
    for time in range(t_init,t_end,t_step):
        measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect(outputdir+'cosi-tsdetect_'+str(time)+'.txt')
        
        args=['--config',cosipy_yaml_input, '--config_group', 'threemlfit_'+modelname,'--override',var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,'--overwrite', '--suffix',modelname+'_'+str(time),'--output-dir',outputdir,'--tstart',str(time),'--tstop',str(time+t_scan_delta)]
        if maxumumTS>50:# to be decided based on statistical arguments
            print("REEADY cosi_threemlfit "+modelname)
            cosi_threemlfit(argv=args)


def build_pdf_file(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    from PIL import Image,ImageDraw,ImageFont
    import os
    import re
    import math
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_cosi_ts_detect,read_trigger_content,read_base_pipeline_params
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    # read max TS on total interval - External trigger
    measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect(directory_output+'cosi-tsdetect_'+str(externalTrigger_start)+'.txt')
    
    # read max TS on single scan intervals 
    maxumumTS_all = []
    maxumum_l = []
    maxumum_b = []
    nameFiles_TS = []
    timeFiles = []
    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".txt"):
            fileName=directory_output+'timescan/'+ f
            nameFiles_TS.append(fileName)
            print(f)

    nameFiles_TS_sorted = sorted(
        nameFiles_TS,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
    
    for name in nameFiles_TS_sorted:
        num = re.findall(r"\d+", name)[0]
        measured_l_tmp,measured_b_tmp,error_coo_tmp,maxumumTS_tmp=read_cosi_ts_detect(name)
        maxumumTS_all.append(maxumumTS_tmp)
        timeFiles.append(num)
        maxumum_l.append(measured_l_tmp)
        maxumum_b.append(measured_b_tmp)
        
    pages = []
    pngs = []
    # read png for scan 
    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".png") and "raw_ts" in f:
            pngs.append(directory_output+'timescan/'+ f)
            print(f)

    pngs_sorted = sorted(
        pngs,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
    # read png for external trigger
    pngs_sorted.append(directory_output+'raw_ts_'+str(externalTrigger_start)+'.png')
    frame_numtot=len(pngs_sorted)

    frameNum=0
    for path in pngs_sorted:
        img = Image.open(path)
        txt = Image.new("RGBA", img.size)
        
        font = ImageFont.truetype("DejaVuSans.ttf", 40)
        font2 = ImageFont.truetype("DejaVuSans.ttf", 30)
        font3 = ImageFont.truetype("DejaVuSans.ttf", 60)

        draw = ImageDraw.Draw(txt)
        if frameNum<frame_numtot-1:
            draw.text((20, 0),'Frame '+str(frameNum) ,font=font,fill=(0, 0, 0, 255))
            draw.text((20, 40),'Time start= ' + str(timeFiles[frameNum]) + ' s' ,font=font2,fill=(0, 0, 0, 255))
            draw.text((20, 80),'Max TS='+str(math.trunc(maxumumTS_all[frameNum]*10)/10),font=font2,fill=(0, 0, 0, 255))
            draw.text((20, 120),'Max l='+str(math.trunc(maxumum_l[frameNum]*10)/10)+' deg',font=font2,fill=(0, 0, 0, 255))
            draw.text((20, 160),'Max b='+str(math.trunc(maxumum_b[frameNum]*10)/10)+' deg',font=font2,fill=(0, 0, 0, 255))

        else:
            draw.text((20, 0),'External trigger',font=font3,fill=(255, 0, 0, 255))# TRUE????????????????????? Existing?
            draw.text((20, 80),'Max TS='+str(math.trunc(maxumumTS*10)/10),font=font2,fill=(0, 0, 0, 255))
            draw.text((20, 120),'Max l='+str(math.trunc(measured_l*10)/10)+' deg',font=font2,fill=(0, 0, 0, 255))
            draw.text((20, 160),'Max b='+str(math.trunc(measured_b*10)/10)+' deg',font=font2,fill=(0, 0, 0, 255))
            draw.text((1100, 60),'External start='+str(math.trunc(externalTrigger_start*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))
            draw.text((1100, 100),'External stop='+str(math.trunc(externalTrigger_stop*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))

        out = Image.alpha_composite(img, txt)
        
        pages.append(out.convert("RGB"))
        frameNum+=1

        
    pages[0].save(
        directory_output+'raw_ts_sequence.pdf',
        save_all=True,
        resolution=200.0,
        format="PDF",
        append_images=pages[1:])

def build_spectral_fit(cosipy_yaml_input,pipeline_input_file,modeltoplot,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    from PIL import Image,ImageDraw,ImageFont
    import os
    import re
    import math
    import sys
    import h5py

    sys.path.append(base_funct)
    from common.funzioni_comuni import read_cosi_ts_detect,read_trigger_content,read_base_pipeline_params,read_spectral_fit_info
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    
    nameFiles_fit = []
    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".h5") and str(modeltoplot) in f:
            fileName=directory_output+'timescan/'+ f
            nameFiles_fit.append(fileName)
            print(f)
            
    nameFiles_fit_sorted = sorted(
        nameFiles_fit,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )

    nameFiles_fit_external = []
    for f in os.listdir(directory_output):
        if f.lower().endswith(".h5") and str(modeltoplot) in f:
            fileName=directory_output+ f
            nameFiles_fit_external.append(fileName)
            print(f)
            
    nameFiles_fit_external_sorted = sorted(
        nameFiles_fit_external,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
    
    for list_tmp in nameFiles_fit_external_sorted:
        nameFiles_fit_sorted.append(list_tmp)
    
    pages = []
    pngs1 = []
    timeFiles = []
    # read png for scan 
    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".png") and "raw_spectrum_"+str(modeltoplot) in f:
            pngs1.append(directory_output+'timescan/'+ f)
            print(f)

    pngs_sorted = sorted(
        pngs1,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
    # read png for external trigger
    pngs_sorted.append(directory_output+'raw_spectrum_'+modeltoplot+'_'+str(externalTrigger_start)+'.png')

    for name in nameFiles_fit_sorted:
        num = re.findall(r"\d+", name)[0]
        timeFiles.append(num)

    
    frame_numtot=len(pngs_sorted)
    num_fitmodels=len(nameFiles_fit_external_sorted)

    frameNum=0
        
    for path in pngs_sorted:
        img = Image.open(path)
        txt = Image.new("RGBA", img.size)
        
        font = ImageFont.truetype("DejaVuSans.ttf", 10)
        font2 = ImageFont.truetype("DejaVuSans.ttf", 9)
        font3 = ImageFont.truetype("DejaVuSans.ttf", 20)

        draw = ImageDraw.Draw(txt)
        draw.text((600, 30),str(modeltoplot) ,font=font3,fill=(255, 0, 0, 255))

        if frameNum<frame_numtot-1:
            draw.text((450, 60), "Only frames with TS above threshold " ,font=font2,fill=(255, 0, 0, 255))

            name_var,value_var,errneg_var,errpos_var,unit_var = read_spectral_fit_info(frameNum,directory_output+'timescan/',modeltoplot)

            draw.text((90, 70),'Time start= ' + str(timeFiles[frameNum]) + ' s' ,font=font2,fill=(0, 0, 0, 255))
            for uu in range(len(value_var)):
                draw.text((90, 100+uu*20),str(name_var[uu].decode("utf-8")[-15:]) + ' = ' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((200, 100+uu*20),str(math.trunc(value_var[uu]*1000)/1000) + ' (' + str(math.trunc(errneg_var[uu]*10000)/10000) + ',' + str(math.trunc(errpos_var[uu]*10000)/10000) + ')' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((320, 100+uu*20),str(unit_var[uu].decode("utf-8")) ,font=font2,fill=(0, 0, 0, 255))

        else:
            name_var,value_var,errneg_var,errpos_var,unit_var = read_spectral_fit_info(0,directory_output,modeltoplot)
            draw.text((100, 80),'External trigger',font=font3,fill=(255, 0, 0, 255))# TRUE????????????????????? Existing?
            draw.text((100, 110),'External start='+str(math.trunc(externalTrigger_start*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))
            draw.text((100, 130),'External stop='+str(math.trunc(externalTrigger_stop*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))
            for uu in range(len(value_var)):
                draw.text((90, 150+uu*20),str(name_var[uu].decode("utf-8")[-15:]) + ' = ' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((200, 150+uu*20),str(math.trunc(value_var[uu]*1000)/1000) + ' (' + str(math.trunc(errneg_var[uu]*10000)/10000) + ',' + str(math.trunc(errpos_var[uu]*10000)/10000) + ')' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((320, 150+uu*20),str(unit_var[uu].decode("utf-8")) ,font=font2,fill=(0, 0, 0, 255))

        out = Image.alpha_composite(img, txt)

        pages.append(out.convert("RGB"))
        frameNum+=1

    pages[0].save(
        directory_output+'raw_spectrum_'+modeltoplot+'_sequence.pdf',
        save_all=True,
        resolution=200.0,
        format="PDF",
        append_images=pages[1:])

def prepare_alert_external(cosipy_yaml_input,pipeline_input_file,base_funct):
    import sys
    import os
    import re

    sys.path.append(base_funct)
    from common.funzioni_comuni import read_cosi_ts_detect,read_trigger_content,read_base_pipeline_params
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,directory_output = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    # read max TS on total interval - External trigger
    measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect(directory_output+'cosi-tsdetect_'+str(externalTrigger_start)+'.txt')

    nameFiles_TS = []
    trigger_TS = []
    trigger_l = []
    trigger_b = []

    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".txt"):
            fileName=directory_output+'timescan/'+ f
            nameFiles_TS.append(fileName)
            print(f)

    nameFiles_TS_sorted = sorted(
        nameFiles_TS,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
  
    with open(directory_output+"Pseudo_alert.txt", "w") as f:
        print('Alert from external trigger ',file=f)
        print('Origin XYZ (Fast pipeline, GCN...)',file=f)
        
        if maxumumTS>50:
            print('Confirmed_ext ',1,file=f)
            print('timeStart ',externalTrigger_start,file=f)
            print('timeStart ',externalTrigger_stop,file=f)
            print('Galactic_lat ', measured_l,file=f)
            print('Galactic_long ',measured_b,file=f)
            print('Resolution ',error_coo,file=f)
            print('Max_TS= ',maxumumTS,file=f)
        else:
            print('Confirmed_ext ',0,file=f)

        print('',file=f)
        print('#############################',file=f)
        print('Alert from TSmap scan ',file=f)
        time_frame=0
        number_trigger_frames=0
        for file in nameFiles_TS_sorted:
            measured_l_tmp,measured_b_tmp,error_coo_tmp,maxumumTS_tmp=read_cosi_ts_detect(file)
            timestart=t_scan_start_source + (time_frame*t_scan_delta)
            timestop= timestart + t_scan_delta
            if maxumumTS_tmp>50:
                print('Independent ',1,file=f)
                print('timeStart ',timestart,file=f)
                print('timeStart ',timestop,file=f)
                print('Galactic_lat ', measured_l_tmp,file=f)
                print('Galactic_long ',measured_b_tmp,file=f)
                print('Resolution ',error_coo_tmp,file=f)
                print('Max_TS= ',maxumumTS_tmp,file=f)
                print('',file=f)
                number_trigger_frames+=1
                
            time_frame+=1
        if number_trigger_frames==0:
            print('Independent ',0,file=f)
            
# Default arguments for the DAG
default_args = {
    'owner': 'gamma',
}

with DAG(
    dag_id="PipelineComprehensive_v3",
    default_args=default_args,
    description="Run version 3 comprehensive pipeline - python version",
    schedule_interval=None,   # run on-demand
    catchup=False,
    tags=["cosifest", "handson", "tutorials"],
) as dag:

    
    cleanup_format = ExternalPythonOperator(
        task_id="cleanup_and_format",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=cleanup_and_format,
    )
    
    executebinning_bck = ExternalPythonOperator(
        task_id="execute_bindata_bck_model",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_bck_model,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],

    )
    
    executebinning_grb = ExternalPythonOperator(
        task_id="execute_bindata_grb",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_grb,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
    
    tsmap_singlesource = ExternalPythonOperator(
        task_id="execute_tsmap_externaltrigger",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_tsmap_externaltrigger,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
    
    tsmap_singlesource_scan = ExternalPythonOperator(
        task_id="execute_tsmap_scan",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_tsmap_scan,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
    
    build_pdf = ExternalPythonOperator(
        task_id="build_pdf",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=build_pdf_file,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
    
    fittask_scan = []
    for i in range(2):
        modelname=""
        if i==0:
            modelname="fit_spectrum_pw_scan"
        if i==1:
            modelname="fit_spectrum_band_scan"

        t = ExternalPythonOperator(
        task_id=modelname,
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_threemlfit,
        op_args=[cosipy_yaml_input_file,pipeline_configs,dir_out+'timescan/',i,1,base_funct_dir],
        )
        fittask_scan.append(t)

    fittask_externaltrigger = []
    for i in range(2):
        modelname=""
        if i==0:
            modelname="fit_spectrum_pw_externaltrigger"
        if i==1:
            modelname="fit_spectrum_band_externaltrigger"

        t = ExternalPythonOperator(
        task_id=modelname,
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_threemlfit,
        op_args=[cosipy_yaml_input_file,pipeline_configs,dir_out,i,0,base_funct_dir],
        )
        fittask_externaltrigger.append(t)
    
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=dir_out+"InputFiles/galbk_grbdc3_full.fits",  # percorso del file da monitorare
        poke_interval=5,   # controlla ogni 5 secondi
        timeout=60 * 3,    # smette dopo 3 minuti
        mode="poke",      # oppure "reschedule" per ridurre il carico
        soft_fail=True
    )
    
    wait_external_trigger = FileSensor(
        task_id="wait_external_trigger",
        filepath=dir_out+"InputFiles/externalTriggerInfos.txt",  # percorso del file da monitorare
        poke_interval=5,   # controlla ogni 5 secondi
        timeout=60 * 3,    # smette dopo 3 minuti
        mode="poke",      # oppure "reschedule" per ridurre il carico
        soft_fail=True
    )
    
    
    merge_spectral_fit_multiple = []
    for i in range(2):
        modelname=""
        if i==0:
            modelname="pw"
        if i==1:
            modelname="band"

        t = ExternalPythonOperator(
        task_id='merge_spectral_fit_plots_'+modelname,
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=build_spectral_fit,
        op_args=[cosipy_yaml_input_file,pipeline_configs,modelname,base_funct_dir],
        )
        merge_spectral_fit_multiple.append(t)
    
    prepare_alert_external_exe = ExternalPythonOperator(
        task_id="prepare_alert_external",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=prepare_alert_external,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
        
    
    join = EmptyOperator(task_id="join")

    
     
    wait_for_file>>cleanup_format
    cleanup_format>>[executebinning_bck,executebinning_grb]>>join
    join>>[wait_external_trigger>>tsmap_singlesource,tsmap_singlesource_scan] #,anomalydetection]
    tsmap_singlesource_scan>>fittask_scan
    tsmap_singlesource>>fittask_externaltrigger
    fittask_scan>>build_pdf
    fittask_externaltrigger>>build_pdf>>merge_spectral_fit_multiple>>prepare_alert_external_exe
    

    '''
    choice = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_fit_method
    )
    
    '''
    '''
    anomalydetection = ExternalPythonOperator(
        task_id="anomaly_detection",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=AnomalyDetection_autoencoder,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )
    '''
'''
def choose_fit_method():
    method_choose=-1
    f_file = open('/home/gamma/workspace/data/transient/config_pipeline.txt')
    content_file = f_file.read().splitlines()
    for line_file in content_file:
        line_object=line_file.split()
        if line_object[0]=="FitMethod":
            method_choose=int( line_object[1])
    print("Method ",method_choose," chosen")
    if method_choose==1:
        return "execute_bindata_threemlfit1"
    if method_choose==2:
        return "execute_bindata_threemlfit2"
'''
