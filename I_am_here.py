
import pyautogui
import datetime
import math
import time
import winsound
from msvcrt import getch

frequency = 2500  # Set Frequency To 2500 Hertz
duration = 1000  # Set Duration To 1000 ms == 1 second

start = datetime.datetime.now()
a=0
b=1 
c=0
delay=5
durata=input("Quanti minuti ti devo coprire?\n")
while a==0:
    now= datetime.datetime.now() 
    b=b*(-1)
    c+=1
    coordinate=[1120-100*b,520]
    pyautogui.moveTo(coordinate[0],coordinate[1])
    time.sleep(delay)
    pyautogui.click(coordinate[0],coordinate[1])
    if (c*delay)%60==0:
        print(f'{int(int(durata)-(delay*c)/60)} minuti rimasti')
    if c*delay>int(durata)*60:
        a=1

winsound.Beep(frequency, duration) 