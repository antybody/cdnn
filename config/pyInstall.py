import os
def getTxt():
        txt = open("./menu.txt","r").read()
        for dd in ",":
                txt=txt.replace(dd," ")
        return txt
kusTxt=getTxt()
words= kusTxt.split()

for word in words:
        os.system("pip install "+word)
        print("成功安装")
