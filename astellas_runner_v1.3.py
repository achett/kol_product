import queue
import threading
import time
import PySimpleGUI as sg
import os.path
import configparser
import os
import requests
import math
import numpy as np
import pandas as pd
import warnings
import configparser
from function_PubMed import *
from function_ClinicalTrials import *
from function_KOL import *
from function_deliverable import *


sg.theme('GreenTan')

BAR_MAX = 100

cwd = os.getcwd()
fname = 'astellas.png'

with open('{}/{}'.format(cwd, fname), "rb") as fh:
    image1 = fh.read()

text_box_size = 70
text_size = 20
def parse_properties_file(file):
    config = configparser.RawConfigParser()
    config.read(file)
    return config

# First the window layout in 2 columns

config_location = [
    [
        sg.Text("Configuration File", size=(text_size, 1),),
        sg.In(size=(text_box_size, 1), enable_events=True, key="-CONFIG-"),
        sg.FileBrowse(),
    ],
]

export_location = [
    [
        sg.Text("Export Directory", size=(text_size, 1),),
        sg.In(size=(text_box_size, 1), enable_events=True, key="-EXPORT-"),
        sg.FolderBrowse(key="-EXP_BTN-"),
    ],
]

# For now will only show the name of the file that was chosen
publication_filters = [
    [sg.Text("Publication Filters")],
    
    [sg.Text("Years", size=(text_size, 1),),
    sg.In(size=(text_box_size, 1), enable_events=True, key="-P-YEARS-"),],
    
    [sg.Text("Search Terms", size=(text_size,1),),
    sg.In(size=(text_box_size, 1), enable_events=True, key="-P-SEARCH-TERMS-"),],
]

# For now will only show the name of the file that was chosen
clinical_filters = [
    [sg.Text("Clinical Trial Filters")],
    [sg.Text("Years", size=(text_size, 1),),
    sg.In(size=(text_box_size, 1), enable_events=True, key="-C-YEARS-"),],
    [sg.Text("Search Terms", size=(text_size, 1),),
    sg.In(size=(text_box_size, 1), enable_events=True, key="-C-SEARCH-TERMS-"),],
]

# ----- Full layout -----
layout = [
    [sg.Image(filename='{}/{}'.format(cwd, "astellas.gif"), key='-IMAGE-', size=(200, 80), pad=(290,1))],
    [config_location],
    [sg.HSeparator()],
    [publication_filters],
    [sg.HSeparator()],
    [clinical_filters],
    [sg.HSeparator()],
    [export_location],
    [sg.Button("Save and Run", size=(text_size, 1), pad=(310,1), enable_events=True,key="-SAVE-RUN-")],
    [sg.ProgressBar(BAR_MAX, orientation='h', size=(70, 10), key='-PROGRESS-')],
    [sg.Output(size=(107, 5))]
]

window = sg.Window("astellas KOL App v1.3", layout, icon='astellas_icon.ico')

def long_operation_thread(file,op_dir,gui_queue):
    window['-PROGRESS-'].update(5)
            
    year,query_arg,exp,path = initialize_vars(file,op_dir)
    window['-PROGRESS-'].update(15)
    
    df, ct, pm = process_PM_CT(year,query_arg,exp,path)
    window['-PROGRESS-'].update(50)
    
    npi = final_process(path,df)
    window['-PROGRESS-'].update(75)
    
    deliverable(path)
    window['-PROGRESS-'].update(100)    
    
    gui_queue.put('** Done **')  # put a message into queue for GUI

def the_gui():
    """
    Starts and executes the GUI
    Reads data from a Queue and displays the data to the window
    Returns when the user exits / closes the window
    """

    gui_queue = queue.Queue()  # queue used to communicate between the gui and the threads
    op_dir = cwd + '\output'
    # --------------------- EVENT LOOP ---------------------
    while True:        
        event, values = window.Read()    
        
        # print(event)
        # print(values)
        
        if event == "Exit" or event == sg.WIN_CLOSED:
            break
        if event == "-CONFIG-":
            file = values["-CONFIG-"]
            try:
                # Get list of files in folder
                config = parse_properties_file(file)
            
            except:
            	#Exception
                file_list = []
            			
            window["-P-YEARS-"].update(config["Publication Filters"]["Years"])
            window["-P-SEARCH-TERMS-"].update(config["Publication Filters"]["Search Terms"])
            window["-C-YEARS-"].update(config["Clinical Filters"]["Years"])
            window["-C-SEARCH-TERMS-"].update(config["Clinical Filters"]["Search Terms"])
            window["-EXPORT-"].update(op_dir)
            
        if event == "-EXPORT-":
            op_dir = values["-EXP_BTN-"]
            #print(op_dir)
            window["-EXPORT-"].update(op_dir)
        elif event == "-SAVE-RUN-":
            config["Publication Filters"]["Years"] = window["-P-YEARS-"].get()
            config["Publication Filters"]["Search Terms"] = window["-P-SEARCH-TERMS-"].get()
            config["Clinical Filters"]["Years"] = window["-C-YEARS-"].get()
            config["Clinical Filters"]["Search Terms"] = window["-C-SEARCH-TERMS-"].get()
        
            with open(file, 'w') as configfile:
                config.write(configfile)
        
            threading.Thread(target=long_operation_thread, args=(file,op_dir,gui_queue,), daemon=True).start()
        
        try:
            message = gui_queue.get_nowait()
        except queue.Empty:             # get_nowait() will get exception when Queue is empty
            message = None              # break from the loop if no more messages are queued up
            
        #print(message)
        
        # if message received from queue, display the message in the Window
        #if message:
        #    print(message)
            # if user exits the window, then close the window and exit the GUI func
            #window.Close()


if __name__ == '__main__':
    the_gui()
    print('Exiting Program')