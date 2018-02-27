FROM continuumio/anaconda3:latest
LABEL maintainer="chandler <chandler605@outlook.com>"

LABEL REFRESH_AT 2018_02_27

ENV  PIP_CONFIG_FILE /etc/pip.conf

COPY ./dockerDependency/chinaSources/sources.list /etc/apt/
COPY ./dockerDependency/chinaSources/pip.conf /etc/

RUN apt-get update && \
    apt-get upgrade -y
    
   
# install dependency
# TA-Lib
RUN apt-get install -y gcc make&& \ 
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr && \
  make && \
  make install && \
  cd .. && \
  rm -R ta-lib ta-lib-0.4.0-src.tar.gz


  
#others  
RUN  conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/ && \
     conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/  && \
     conda config --set show_channel_urls yes && \
     conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud/conda-forge/ && \
     conda install -y jupyterlab && \
     pip install --no-cache-dir lxml bs4 requests requests-file pandas_datareader&& \
     pip install --no-cache-dir tushare && \
     pip install --no-cache-dir ta-lib 
     

 

LABEL io.k8s.description="Greenseer imagine" \
    io.k8s.display-name="Greenseer imagine" \
    io.openshift.expose-services="8000:http" \
    io.openshift.tags="jupyter notebook" \
    # this label tells s2i where to find its mandatory scripts
    # (run, assemble, save-artifacts)
    io.openshift.s2i.scripts-url="image:///usr/libexec/s2i"
    

ENV GREENSEER_HOME /opt/greenseer


RUN mkdir -p  $GREENSEER_HOME && \
    chown 10001:10001 $GREENSEER_HOME && \
    chmod -R 777 $GREENSEER_HOME && \
    mkdir -p /.local && \
    chown 10001:10001 /.local  && \
    chmod -R 777 /.local 
    
COPY ./dockerDependency/jupyter_notebook_config.py $GREENSEER_HOME
COPY ./dockerDependency/s2i/bin/ /usr/libexec/s2i
ENV JUPYTER_CONFIG_DIR /opt/greenseer/jupyter


USER 10001


