from airflow.operators.python import ExternalPythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

from airflow import DAG

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy_laura/bin/python"
cosipy_yaml_input_file = "/home/gamma/workspace/data/apps/pipeline_zero4_galactic_large.yaml"
pipeline_configs= "/home/gamma/workspace/data/apps/config_pipeline2.txt"

def execute_bindata_bck_model(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_base_pipeline_params

    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    args=['--config',cosipy_yaml_input,'--config_group','bindata_bk','--overwrite', '--suffix','Background_Model','--output-dir','/home/gamma/workspace/data/apps/','--tmin', str(t_scan_start), '--tmax', str(t_scan_stop)]
    cosi_bindata (argv=args)


def execute_bindata_grb(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_base_pipeline_params
    
    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    args=['--config',cosipy_yaml_input,'--config_group','bindata_soubk','--tmin', str(t_scan_start), '--tmax', str(t_scan_stop),'--overwrite', '--suffix','galbk_grbdc3','--output-dir','/home/gamma/workspace/data/apps/']
    cosi_bindata (argv=args)
    subprocess.run('cd /home/gamma/workspace/data/apps/ && gunzip -f /home/gamma/workspace/data/apps/tsel_unbinned_data_galbk_grbdc3.fits.gz', shell=True) # attenzione!!!!!!!!!!!!!!!!!! da correggere in cosipy!

def execute_tsmap_singlesource(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_base_pipeline_params,read_trigger_content

    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop = read_trigger_content(content_trigger)
    
    args=['--config',cosipy_yaml_input,'--output-dir','/home/gamma/workspace/data/apps/','--overwrite','--tstart', str(externalTrigger_start), '--tstop', str(externalTrigger_stop)]
    with open("/home/gamma/workspace/data/apps/cosi-tsdetect.txt", "w") as f:
        with redirect_stdout(f):
            cosi_tsdetect (argv=args)

def execute_tsmap_singlesource_scan(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    import subprocess
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_base_pipeline_params
    
    subprocess.run('mkdir /home/gamma/workspace/data/apps/timescan', shell=True)
    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    
    fileNum=0
    print('|||||||||||||||||||||||||||||||||||||||||||||||',t_scan_start,t_scan_stop,t_scan_delta)
    for time in range(t_scan_start,t_scan_stop,t_scan_delta):
        outputFile='/home/gamma/workspace/data/apps/timescan/cosi-tsdetect_' + str(fileNum) + '.txt'
        newpngFileName='/home/gamma/workspace/data/apps/timescan/raw_ts_' + str(fileNum) + '.png'
        tstart=time
        tstop=time+t_scan_delta
        args=['--config',cosipy_yaml_input,'--output-dir','/home/gamma/workspace/data/apps/timescan','--overwrite','--tstart', str(tstart), '--tstop', str(tstop)]
        with open(outputFile, "w") as f:
            with redirect_stdout(f):
                cosi_tsdetect (argv=args)
                subprocess.run('mv /home/gamma/workspace/data/apps/timescan/raw_ts.png '+newpngFileName, shell=True)
        fileNum+=1

def AnomalyDetection_autoencoder(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    import torch
    import subprocess
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_base_pipeline_params,read_file_histo
    from histpy import Histogram
    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)
    
    #data_full     = Histogram.open("/home/gamma/workspace/data/apps/tsel_binned_data_galactic_Background_forTraining_BACKONLY.hdf5")
    timeBin = 1
    t0_test=12000
    t0_dataset=t0_test
    durationTot=1000
    durationEvent = 100
    numberEventstest = 10
    binNumTime=durationEvent/timeBin
    resolutionImage = 50
    FileName="/home/gamma/workspace/data/apps/FileOut_test_back_full_backgr"
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
    state = torch.load('/home/gamma/workspace/data/apps/Model_AnomalyDetection.pth')
    model.load_state_dict(state)
    '''
    fileNum=0
    for time in range(t_scan_start,t_scan_stop):
        outputFile='/home/gamma/workspace/data/apps/timescan/cosi-AD_detect_' + str(fileNum) + '.txt'
    '''

        
def execute_bindata_threemlfit1_multi_new(cosipy_yaml_input,numfile,pipeline_input_file,fitmodel):
    import cosipy
    from cosipy.pipeline.task.task import cosi_threemlfit
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_cosi_ts_detect,read_base_pipeline_params,format_override_val
    t_scan_start,t_scan_stop,t_scan_delta,content_trigger = read_base_pipeline_params(pipeline_input_file)

    measured_l=float(0.)
    measured_b=float(0.)
    error_coo=float(0.)
    maxumumTS=float(0.)
    measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect('/home/gamma/workspace/data/apps/timescan/cosi-tsdetect_'+str(numfile)+'.txt')
    var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,modelname = format_override_val(fitmodel,measured_l,measured_b,error_coo)
    
    startingtime= float(t_scan_start) + float(numfile) * float(t_scan_delta)
    stoppingtime= float(t_scan_start) + float(numfile + 1.) * float(t_scan_delta)
    args=['--config',cosipy_yaml_input, '--config_group', 'threemlfit_'+modelname,'--override',var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,'--overwrite', '--suffix',modelname+'_'+str(numfile),'--output-dir','/home/gamma/workspace/data/apps/timescan','--tstart',str(startingtime),'--tstop',str(stoppingtime)]
    if maxumumTS>50:# to be decided based on statistical arguments
        print("REEADY cosi_threemlfit "+modelname)
        cosi_threemlfit(argv=args)

    
def execute_bindata_threemlfit1_new(cosipy_yaml_input,fitmodel):
    import cosipy
    from cosipy.pipeline.task.task import cosi_threemlfit
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_cosi_ts_detect,format_override_val
    
    measured_l=float(0.)
    measured_b=float(0.)
    error_coo=float(0.)
    maxumumTS=float(0.)
    measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect('/home/gamma/workspace/data/apps/cosi-tsdetect.txt')
    var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,modelname = format_override_val(fitmodel,measured_l,measured_b,error_coo)
    
    args=['--config',cosipy_yaml_input, '--config_group', 'threemlfit_'+modelname,'--override',var_override1,var_override2,var_override3,var_override4,var_override5,var_override6,var_override7,var_override8,'--overwrite', '--suffix',modelname,'--output-dir','/home/gamma/workspace/data/apps/']
    if maxumumTS>50:
        print("READY cosi_threemlfit "+modelname)
        cosi_threemlfit(argv=args)

def build_pdf_file(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    from PIL import Image,ImageDraw,ImageFont
    import os
    import re
    import math
    import sys
    sys.path.append("/home/gamma/workspace/data/apps/")
    from common.funzioni_comuni import read_cosi_ts_detect

    measured_l,measured_b,error_coo,maxumumTS=read_cosi_ts_detect('/home/gamma/workspace/data/apps/cosi-tsdetect.txt')
    pages = []
    pngs = []

    for f in os.listdir('/home/gamma/workspace/data/apps/timescan/'):
        if f.lower().endswith(".png"):
            pngs.append('/home/gamma/workspace/data/apps/timescan/'+ f)
            print(f)

    pngs_sorted = sorted(
        pngs,
        key=lambda f: int(re.findall(r'\d+', f)[-1])
    )
    pngs_sorted.append('/home/gamma/workspace/data/apps/raw_ts.png')
    frame_numtot=len(pngs_sorted)

    frameNum=0
    for path in pngs_sorted:
        img = Image.open(path)
        txt = Image.new("RGBA", img.size)
        
        font = ImageFont.truetype("DejaVuSans.ttf", 40)
        draw = ImageDraw.Draw(txt)
        if frameNum<frame_numtot-1:
            draw.text((50, 50),'Frame number '+str(frameNum),font=font,fill=(0, 0, 0, 255))
        else:
            draw.text((50, 50),'External input',font=font,fill=(0, 0, 0, 255))# TRUE????????????????????? Existing?
            draw.text((50, 120),'Max TS='+str(math.trunc(maxumumTS*10)/10),font=font,fill=(0, 0, 0, 255))
        out = Image.alpha_composite(img, txt)
        
        pages.append(out.convert("RGB"))
        frameNum+=1

        
    pages[0].save(
        '/home/gamma/workspace/data/apps/raw_ts_sequence.pdf',
        save_all=True,
        resolution=200.0,
        format="PDF",
        append_images=pages[1:])

'''
def choose_fit_method():
    method_choose=-1
    f_file = open('/home/gamma/workspace/data/apps/config_pipeline.txt')
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

    executebinning_bck = ExternalPythonOperator(
        task_id="execute_bindata_bck_model",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_bck_model,
        op_args=[cosipy_yaml_input_file,pipeline_configs],

    )

    executebinning_grb = ExternalPythonOperator(
        task_id="execute_bindata_grb",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_grb,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )
    
    tsmap_singlesource = ExternalPythonOperator(
        task_id="execute_tsmap_singlesource",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_tsmap_singlesource,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )
    
    tsmap_singlesource_scan = ExternalPythonOperator(
        task_id="execute_tsmap_singlesource_scan",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_tsmap_singlesource_scan,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )

    anomalydetection = ExternalPythonOperator(
        task_id="anomaly_detection",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=AnomalyDetection_autoencoder,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )
 
    build_pdf = ExternalPythonOperator(
        task_id="build_pdf",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=build_pdf_file,
        op_args=[cosipy_yaml_input_file,pipeline_configs],
    )
    
    fittask_simple = []
    for i in range(2):
        modelname=""
        if i==0:
            modelname="execute_bindata_threemlfit1_pw_new"
        if i==1:
            modelname="execute_bindata_threemlfit1_band_new"

        t = ExternalPythonOperator(
        task_id=modelname,
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=execute_bindata_threemlfit1_new,
        op_args=[cosipy_yaml_input_file,i],
        )
        fittask_simple.append(t)

    fittask_multi = []
    for i in range(5):
        for u in range(2):
            modelname=""
            if u==0:
                modelname="execute_bindata_threemlfit1_multi_pw_new_"+str(i)
            if u==1:
                modelname="execute_bindata_threemlfit1_multi_band_new_"+str(i)
            t = ExternalPythonOperator(
                
                task_id=modelname,
                python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
                python_callable=execute_bindata_threemlfit1_multi_new,
                op_args=[cosipy_yaml_input_file,i,pipeline_configs,u],

            )
            fittask_multi.append(t)
    
    '''
    choice = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_fit_method
    )
    '''
    
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/home/gamma/workspace/data/apps/InputFiles/galbk_grbdc3_full_110605183.fits",  # percorso del file da monitorare
        poke_interval=5,   # controlla ogni 5 secondi
        timeout=60 * 3,    # smette dopo 3 minuti
        mode="poke",      # oppure "reschedule" per ridurre il carico
        soft_fail=True
    )
    
    wait_external_trigger = FileSensor(
        task_id="wait_external_trigger",
        filepath="/home/gamma/workspace/data/apps/InputFiles/externalTriggerInfos.txt",  # percorso del file da monitorare
        poke_interval=5,   # controlla ogni 5 secondi
        timeout=60 * 3,    # smette dopo 3 minuti
        mode="poke",      # oppure "reschedule" per ridurre il carico
        soft_fail=True
    )
    
    join = EmptyOperator(task_id="join")
    
    wait_for_file>>[executebinning_bck,executebinning_grb]>>join
    join>>[wait_external_trigger>>tsmap_singlesource,tsmap_singlesource_scan,anomalydetection]
    tsmap_singlesource>>fittask_simple
    tsmap_singlesource_scan>>fittask_multi
    fittask_simple>>build_pdf
    fittask_multi>>build_pdf
    
    #wait_for_file>>build_pdf
    #wait_for_file>>executebinning_grb
