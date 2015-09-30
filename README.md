# InformationFlow
A script I wrote to parse tweets archived in a text file and write them to Microsoft Access (using Python)

The context of the project was to get insights into how people on twitter interacted. For this we looked at a bunch of people sharing a url and tried to estimate how the url was diffused through this particular group. 

Given were huge files (ranging from few 100 MB to more than 1 GB) in text format where each line was a tweet in json format. The files were preprocessed to remove incomplete tweets. So, each line will be a valid json. The script is present in parser.py (best viewed in a IDE which preserves the indentation, as python takes indentation very seriously)

The tweets were assumed to be present in a file in the same directory as the parser.py. The file name (inflile.txt) was hardcoded into the script and can be found in line 129: "with open("infile.txt") as f:"

The database connection parameters can be found in lines 115 - 117. The scrip also logs a number in a log file for each 1000 tweets it processes. This is good for debugging purposes. The log file (log.txt) details can be found in line 119.









