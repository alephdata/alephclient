FROM alephdata/followthemoney
RUN pip3 install alephclient
CMD alephclient
