from airflow.operators.python import ExternalPythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow import DAG
import sys

EXTERNAL_PYTHON = "/home/gamma/envs/cosipy_laura/bin/python"

cosipy_yaml_input_file = "/home/gamma/workspace/data/transient/pipeline_zero4_galactic_large_2.yaml"
pipeline_configs= "/home/gamma/workspace/data/transient/config_pipeline2.txt" #_GRB09.txt"
base_funct_dir="/home/gamma/airflow/modules_pool/comprehensive-transient-analysis-pipeline/" # directory where common/ is located

sys.path.append(base_funct_dir)
from common.funzioni_comuni import read_base_pipeline_params
t_1,t_2,t_11,t_22,t_delta,cont_trigg,thresh,dir_out,anomaly_det = read_base_pipeline_params(pipeline_configs)

def cleanup_and_format(cosipy_yaml_input,pipeline_input_file,base_funct):
    # some cleanup and some service directory creation
    from PIL import Image,ImageDraw,ImageFont
    import numpy as np
    import cosipy
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    import subprocess
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params,read_trigger_content
    from threeML import JointLikelihood,DataList,XYLike,Model,PointSource,Constant

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    outputFile=directory_output+'cosi-tsdetect_' + str(externalTrigger_start) + '.txt'
    newpngFileName=directory_output+'raw_ts_' + str(externalTrigger_start) + '.png'
    newpngFileName2=directory_output+'raw_spectrum_pw_' + str(externalTrigger_start) + '.png'
    newpngFileName3=directory_output+'raw_spectrum_band_' + str(externalTrigger_start) + '.png'
    fileName_toClean = directory_output+"cosi-tsdetect_" +  str(externalTrigger_start) + ".txt"
    
    # clean up old files
    subprocess.run('mkdir '+directory_output+'timescan', shell=True)
    subprocess.run('mkdir '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'tsel_* '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'bin* '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'*png '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'timescan/*png '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'timescan/*h5 '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'timescan/*txt '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'*pdf '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv '+directory_output+'*h5 '+directory_output+'../garbagebin', shell=True)
    subprocess.run('mv ' + fileName_toClean + ' '+directory_output+'../garbagebin',shell=True)
    
    # Saves empty images for external trigger branch (in case there is no external trigger)    
    img = Image.new("RGBA",size=(1000,1000),color=(255,255,255,255))
    txt = Image.new("RGBA",size=(1000,1000))
    font = ImageFont.truetype("DejaVuSans.ttf", 40)
    draw = ImageDraw.Draw(txt)
    draw.text((200, 500),'No external trigger received! ',font=font,fill=(0, 0, 0, 255))
    out = Image.alpha_composite(img, txt)
    out.save(newpngFileName)
    out.save(newpngFileName2)
    out.save(newpngFileName3)

    # create empty txt file
    file_empty = open(fileName_toClean,"w")
    print("Maximum TS= 0",file=file_empty)
    print("Galactic coordinate at maximum TS: l=0, b=0",file=file_empty)
    print("Linear Size of TS map pixel: 0",file=file_empty)
    file_empty.close()


    # create empty h5 files
    x = np.array([1.0])
    y = np.array([0.0])
    yerr = np.array([1.0])
    plugin = XYLike("single_point", x, y, yerr)
    plugins = DataList(plugin)
    model = Model(
        PointSource(
            "src",
            0, 0,
            spectral_shape=Constant()
        )
    )
    model.src.spectrum.main.value = 0.0
    
    like = JointLikelihood(model, plugins, verbose=False)
    like.fit()
    results=like.results
    results.write_to(directory_output+"results_pw_"+str(externalTrigger_start)+".h5",as_hdf=True)
    results.write_to(directory_output+"results_band_"+str(externalTrigger_start)+".h5",as_hdf=True)


    
def execute_bindata_bck_model(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)

    args=['--config',cosipy_yaml_input,'--config_group','bindata_bk','--overwrite', '--suffix','Background_Model','--output-dir',directory_output,'--tmin', str(t_scan_start_back), '--tmax', str(t_scan_stop_back)]
    cosi_bindata (argv=args)

def execute_bindata_grb(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params
    
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    args=['--config',cosipy_yaml_input,'--config_group','bindata_soubk','--overwrite', '--suffix','galbk_grbdc3','--output-dir',directory_output,'--tmin', str(t_scan_start_source), '--tmax', str(t_scan_stop_source)]
    cosi_bindata (argv=args)


def check_external_trigger(cosipy_yaml_input,pipeline_input_file,base_funct):
    from contextlib import redirect_stdout
    import sys
    import subprocess
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params,read_trigger_content
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    if flag_trigger in ("true", "1", "yes"):
        return "execute_tsmap_externaltrigger"
    else:
        return None

def execute_tsmap_externaltrigger(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    from contextlib import redirect_stdout
    from cosipy.pipeline.task.task import cosi_tsdetect
    import sys
    import subprocess

    sys.path.append(base_funct)
    from common.funzioni_comuni import read_base_pipeline_params,read_trigger_content

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
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
    
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
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
    import torch.nn as nn
    import subprocess
    from contextlib import redirect_stdout
    import matplotlib.pyplot as plt
    import numpy as np
    from cosipy.pipeline.task.task import cosi_tsdetect
    from cosipy.pipeline.src.io import load_ori
    import sys
    sys.path.append(base_funct)
    import healpy as hp
    from common.funzioni_comuni import read_base_pipeline_params,read_file_histo,read_trigger_content,read_anomaly_detection_config
    from common.models import SimpleCNN3
    from astropy.time import Time
    from histpy import Histogram
    from mhealpy import HealpixMap
    from scoords import SpacecraftFrame

    criterion = nn.CrossEntropyLoss()

    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    input_file_name,model_file,resolutionImage,timeBin,plotting_window,ori_file,true_b,true_l=read_anomaly_detection_config(file_anomaly_detection)
    
    FileName=directory_output+"FileOut_test_back_full_backgr"
    numberEventstest=1
    t0_test=0
    
    data_full     = Histogram.open(directory_output+str(input_file_name))
    binNumTime = int(data_full.project('Time').nbins)
    imagePlotX_Z_t_test = read_file_histo(data_full,t0_test,t0_test+binNumTime,resolutionImage,binNumTime,FileName,binNumTime,numberEventstest)

    model = SimpleCNN3()
    state = torch.load(base_funct+str(model_file))
    model.load_state_dict(state)

    lossTensor = torch.zeros(int(binNumTime))
    frameNum = torch.zeros(int(binNumTime))
    
    lossMap_3D_tmp = torch.zeros((numberEventstest,resolutionImage,resolutionImage,resolutionImage,int(binNumTime)))
    signalMap_3D_tmp = torch.zeros((numberEventstest,resolutionImage,resolutionImage,resolutionImage,int(binNumTime)))
    modelMap_3D_tmp = torch.zeros((numberEventstest,resolutionImage,resolutionImage,resolutionImage,int(binNumTime)))

    max_loss=0
    max_loss_time=0
    for t in range(0,binNumTime):
        image_for_input = imagePlotX_Z_t_test[:,:,:,:,int(t)]
        outTEST=model(image_for_input)
        lossPlot = criterion(outTEST,image_for_input)
        frameNum[int(t)]=int(t)
        lossTensor[int(t)]=float(lossPlot)
        outClone=outTEST.clone()
        imageClone=image_for_input.clone()
        lossMap_3D_tmp[0:1,:,:,:,int(t)] += (outClone - imageClone)**2
        signalMap_3D_tmp[0:1,:,:,:,int(t)] += imageClone
        modelMap_3D_tmp[0:1,:,:,:,int(t)] += outClone
        if lossPlot>max_loss:
            max_loss=lossPlot
            max_loss_time = int(t)


    print('NumBinsTime ######################################### ',binNumTime)
    skymaptoplot = torch.t(lossMap_3D_tmp[0,:,:,0:10,int(max_loss_time-plotting_window):int(max_loss_time+plotting_window)].sum(dim=3).sum(dim=2))
    
    skymaptoplot_data = torch.t(signalMap_3D_tmp[0,:,:,0:10,int(max_loss_time-plotting_window):int(max_loss_time+plotting_window)].sum(dim=3).sum(dim=2))
    skymaptoplot_data2 = torch.t(signalMap_3D_tmp[0,:,:,0:10,int(max_loss_time+20):int(max_loss_time+30)].sum(dim=3).sum(dim=2))

    skymaptoplot_model = torch.t(modelMap_3D_tmp[0,:,:,0:10,int(max_loss_time-plotting_window):int(max_loss_time+plotting_window)].sum(dim=3).sum(dim=2))
    skymaptoplot_model2 = torch.t(modelMap_3D_tmp[0,:,:,0:10,int(max_loss_time+20):int(max_loss_time+30)].sum(dim=3).sum(dim=2))
    
    arrTest_2 = np.zeros(data_full.project('PsiChi').nbins)
    for i in range(skymaptoplot.shape[0]):
        for ii in range(skymaptoplot.shape[1]):
            ang1=np.deg2rad(float(ii*(180. / resolutionImage)))
            ang2=np.deg2rad(float(i*(360. / resolutionImage)))
            pixel_2=hp.ang2pix(8,ang1,ang2)
            arrTest_2[pixel_2]+=skymaptoplot[i][ii]
            
    
    plt.figure(figsize=(16.03, 10.41) ) 
    plt.plot(frameNum,lossTensor)
    plt.title('Anomaly detection - Loss curve')
    plt.xlabel("Frame number")
    plt.ylabel('Loss curve')
    plt.savefig(directory_output+'loss_curve.png')
    
    plt.figure(figsize=(16.03, 10.41) ) 
    plt.imshow(skymaptoplot.detach().numpy())
    plt.gca().invert_yaxis() 
    plt.title('LossMap - example; Test set')
    plt.xlabel('Bin X')
    plt.ylabel('Bin Y')
    plt.colorbar()
    plt.savefig(directory_output+'imageLoss_2DPLot.png')

    plt.figure(figsize=(16.03, 10.41) ) 
    plt.imshow(skymaptoplot_data.detach().numpy())
    plt.gca().invert_yaxis() 
    plt.title('DataMap - example; Test set')
    plt.xlabel('Bin X')
    plt.ylabel('Bin Y')
    plt.clim(0,10)
    plt.colorbar()
    plt.savefig(directory_output+'imageData_2DPLot.png')
    
    plt.figure(figsize=(16.03, 10.41) ) 
    plt.imshow(skymaptoplot_data2.detach().numpy())
    plt.gca().invert_yaxis() 
    plt.title('DataMap - example backg; Test set')
    plt.xlabel('Bin X')
    plt.ylabel('Bin Y')
    plt.clim(0,10)
    plt.colorbar()
    plt.savefig(directory_output+'imageData_2DPLot_2.png')

    plt.figure(figsize=(16.03, 10.41) ) 
    plt.imshow(skymaptoplot_model.detach().numpy())
    plt.gca().invert_yaxis() 
    plt.title('ModelMap - example; Test set')
    plt.xlabel('Bin X')
    plt.ylabel('Bin Y')
    plt.clim(0,10)
    plt.colorbar()
    plt.savefig(directory_output+'imageModel_2DPLot.png')

    plt.figure(figsize=(16.03, 10.41) ) 
    plt.imshow(skymaptoplot_model2.detach().numpy())
    plt.gca().invert_yaxis() 
    plt.title('ModelMap background- example; Test set')
    plt.xlabel('Bin X')
    plt.ylabel('Bin Y')
    plt.clim(0,10)
    plt.colorbar()
    plt.savefig(directory_output+'imageModel_2DPLot_2.png')

    ori = load_ori(str(directory_output)+str(ori_file))
    tstart=Time(t_scan_start_source, format='unix')
    tstop=Time(t_scan_stop_source, format='unix')
    tmiddle=(t_scan_start_source+t_scan_stop_source)/2.
    m = HealpixMap(nside = 8, coordsys=SpacecraftFrame( attitude = ori.interp_attitude(Time(tmiddle, format='unix')   )))

    for i in range(data_full.project('PsiChi').nbins):
        content =arrTest_2[i]
        m[i]+=content
        
    fig,ax = plt.subplots(subplot_kw = {'projection':'mollview', 'coord':'G'})
    m.plot(ax=ax,vmin=10)

    plt.savefig(directory_output+'imageLoss_GalCoord_rot.png')


    
def cnn_locate(cosipy_yaml_input,pipeline_input_file,base_funct):
    import cosipy
    import torch
    import torch.nn as nn
    import subprocess
    from contextlib import redirect_stdout
    import matplotlib.pyplot as plt
    import numpy as np
    from cosipy.pipeline.task.task import cosi_tsdetect
    from cosipy.pipeline.src.io import load_ori
    import sys
    sys.path.append(base_funct)
    import healpy as hp
    from common.funzioni_comuni import read_base_pipeline_params,read_file_histo,read_trigger_content,read_anomaly_detection_config
    from common.models import SimpleCNN3
    from astropy.time import Time
    from histpy import Histogram
    from mhealpy import HealpixMap
    from scoords import SpacecraftFrame
    
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    input_file_name,model_file,resolutionImage,timeBin,plotting_window,ori_file,true_b,true_l=read_anomaly_detection_config(file_anomaly_detection)
    
    FileName=directory_output+"FileOut_test_3DCNN_"
    numberEventstest=1
    t0_test=0
    
    data_full     = Histogram.open(directory_output+str(input_file_name))
    binNumTime = int(data_full.project('Time').nbins)
    imagePlotX_Z_t_test = read_file_histo(data_full,t0_test,t0_test+binNumTime,resolutionImage,binNumTime,FileName,binNumTime,numberEventstest)
    print('########################### ',imagePlotX_Z_t_test.shape)

     
def execute_threemlfit(cosipy_yaml_input,pipeline_input_file,outputdir,fitmodel,scanvar,base_funct):
    import cosipy
    from cosipy.pipeline.task.task import cosi_threemlfit
    import sys
    sys.path.append(base_funct)
    from common.funzioni_comuni import read_cosi_ts_detect,read_base_pipeline_params,format_override_val,read_trigger_content
    from PIL import Image, ImageDraw, ImageFont
    from threeML import JointLikelihood,DataList,XYLike,Model,PointSource,Constant
    import numpy as np
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    
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
        if maxumumTS>float(threshold_TS):
            print("REEADY cosi_threemlfit "+modelname)
            cosi_threemlfit(argv=args)
        else:            
            img = Image.new("RGBA",size=(1000,1000),color=(255,255,255,255))
            txt = Image.new("RGBA",size=(1000,1000),)
            font = ImageFont.truetype("DejaVuSans.ttf", 40)
            draw = ImageDraw.Draw(txt)
            draw.text((100, 400),"Under threshold!" ,font=font,fill=(0, 0, 0, 255))
            draw.text((100, 500),"Time "+str(time)+' s; model= '+str(modelname),font=font,fill=(0, 0, 0, 255))
            out = Image.alpha_composite(img, txt)
            out.save(str(outputdir)+"raw_spectrum_"+str(modelname)+"_"+str(time)+".png")

            ###################
            x = np.array([1.0])
            y = np.array([0.0])
            yerr = np.array([1.0])
            plugin = XYLike("single_point", x, y, yerr)
            plugins = DataList(plugin)
            model = Model(
                PointSource(
                "src",
                0, 0,
                spectral_shape=Constant()
                )
            )
            model.src.spectrum.main.value = 0.0
            
            like = JointLikelihood(model, plugins, verbose=False)
            like.fit()
            results=like.results
            results.write_to(str(outputdir)+"results_"+str(modelname)+"_"+str(time)+".h5",as_hdf=True)

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
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
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

    imgsize=0
    frameNum=0
    for path in pngs_sorted:
        img = Image.open(path)
        txt = Image.new("RGBA", img.size)
        imgsize=img.size
        
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

    img_anomaly = Image.open(directory_output+'loss_curve.png')
    txt_anomaly = Image.new("RGBA", img_anomaly.size)
    out_anomaly = Image.alpha_composite(img_anomaly, txt_anomaly)
    out_anomaly=out_anomaly.resize(imgsize)
    pages.append(out_anomaly.convert("RGB"))

    img_anomaly2 = Image.open(directory_output+'imageLoss_2DPLot.png')
    txt_anomaly2 = Image.new("RGBA", img_anomaly2.size)
    out_anomaly2 = Image.alpha_composite(img_anomaly2, txt_anomaly2)
    out_anomaly2=out_anomaly2.resize(imgsize)
    pages.append(out_anomaly2.convert("RGB"))

    img_anomaly4 = Image.open(directory_output+'imageLoss_GalCoord_rot.png')
    txt_anomaly4 = Image.new("RGBA", img_anomaly4.size)
    out_anomaly4 = Image.alpha_composite(img_anomaly4, txt_anomaly4)
    out_anomaly4=out_anomaly4.resize(imgsize)
    pages.append(out_anomaly4.convert("RGB"))
    
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
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
    
    nameFiles_fit = []
    for f in os.listdir(directory_output+'timescan/'):
        if f.lower().endswith(".h5") and str(modeltoplot) in f:
            fileName=directory_output+'timescan/'+ f
            nameFiles_fit.append(fileName)
            print(f)
            
    nameFiles_fit_sorted = sorted(
        nameFiles_fit,
        key=lambda f: int(re.findall(r'\d+', f)[-2])
    )
        
    nameFiles_fit_external = []
    for f in os.listdir(directory_output):
        if f.lower().endswith(".h5") and str(modeltoplot) in f:
            fileName=directory_output+ f
            nameFiles_fit_external.append(fileName)
            print(f)
            
    nameFiles_fit_external_sorted = sorted(
        nameFiles_fit_external,
        key=lambda f: int(re.findall(r'\d+', f)[-2])
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

    #for name in pngs_sorted:
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

            name_var,value_var,errneg_var,errpos_var,unit_var = read_spectral_fit_info(frameNum,directory_output+'timescan/',modeltoplot)

            draw.text((90, 70),'Time start= ' + str(timeFiles[frameNum]) + ' s' ,font=font2,fill=(0, 0, 0, 255))
            if len(value_var)==1:
                draw.text((90, 200),'Fit not converging! ',font=font2,fill=(255, 0, 0, 255))
                
            for uu in range(len(value_var)):
                draw.text((90, 100+uu*20),str(name_var[uu].decode("utf-8")[-15:]) + ' = ' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((200, 100+uu*20),str(math.trunc(value_var[uu]*1000)/1000) + ' (' + str(math.trunc(errneg_var[uu]*10000)/10000) + ',' + str(math.trunc(errpos_var[uu]*10000)/10000) + ')' ,font=font2,fill=(0, 0, 0, 255))
                draw.text((320, 100+uu*20),str(unit_var[uu].decode("utf-8")) ,font=font2,fill=(0, 0, 0, 255))

        else:
            name_var,value_var,errneg_var,errpos_var,unit_var = read_spectral_fit_info(0,directory_output,modeltoplot)
            draw.text((100, 80),'External trigger',font=font3,fill=(255, 0, 0, 255))# TRUE????????????????????? Existing?
            draw.text((100, 110),'External start='+str(math.trunc(externalTrigger_start*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))
            draw.text((100, 130),'External stop='+str(math.trunc(externalTrigger_stop*10)/10)+' s',font=font2,fill=(255, 0, 0, 255))
            if len(value_var)==1:
                draw.text((90, 200),'Fit not converging! ',font=font2,fill=(255, 0, 0, 255))

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
    t_scan_start_source,t_scan_stop_source,t_scan_start_back,t_scan_stop_back,t_scan_delta,content_trigger,threshold_TS,directory_output,file_anomaly_detection = read_base_pipeline_params(pipeline_input_file)
    externalTrigger_start,externalTrigger_stop,flag_trigger = read_trigger_content(content_trigger)
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
        
        if maxumumTS>float(threshold_TS):
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
            if maxumumTS_tmp>float(threshold_TS):
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
    dag_id="PipelineComprehensive_v5",
    default_args=default_args,
    description="Run version 5 comprehensive pipeline - python version",
    schedule_interval=None,   # run on-demand
    catchup=False,
    tags=["cosifest", "handson", "tutorials"],
) as dag:
    
    cleanup_format = ExternalPythonOperator(
        task_id="cleanup_and_format",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=cleanup_and_format,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
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
     
    anomalydetection = ExternalPythonOperator(
        task_id="anomaly_detection",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=AnomalyDetection_autoencoder,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir],
    )
    
    cnn_source_location = ExternalPythonOperator(
        task_id="cnn_source_location",
        python=EXTERNAL_PYTHON,  # Specifica l'interprete dell'ambiente cosipy
        python_callable=cnn_locate,
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
        filepath=dir_out+"InputFiles/data_grbdc3_full.fits",  # percorso del file da monitorare
        poke_interval=5,   # controlla ogni 5 secondi
        timeout=60 * 3,    # smette dopo 3 minuti
        mode="poke",      # oppure "reschedule" per ridurre il carico
        soft_fail=True
    )
    
    check_external_funct = BranchPythonOperator(
        task_id="check_external_trigg",
        python_callable=check_external_trigger,
        op_args=[cosipy_yaml_input_file,pipeline_configs,base_funct_dir]
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
    join>>[check_external_funct,tsmap_singlesource_scan,anomalydetection]
    check_external_funct>>tsmap_singlesource
    tsmap_singlesource_scan>>fittask_scan
    tsmap_singlesource>>fittask_externaltrigger
    anomalydetection>>cnn_source_location>>build_pdf
    fittask_scan>>build_pdf>>merge_spectral_fit_multiple>>prepare_alert_external_exe
    fittask_externaltrigger
    
    #cnn_source_location
