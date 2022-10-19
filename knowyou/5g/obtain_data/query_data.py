# -*- coding: utf-8 -*-

import pymssql

filename = [
'0218500220200929123104MS2.l5g'
]

conn2 = pymssql.connect(host='192.168.2.126',user='sa',password='ky701@YH.com',database='DT_TestData_LTE_2',charset='utf8')
conn1 = pymssql.connect(host='192.168.1.4',user='sa',password='ky701@YH.com',database='DT_TestData_LTE_1',charset='utf8')
conn0 = pymssql.connect(host='192.168.1.4',user='sa',password='ky701@YH.com',database='sysdb3',charset='utf8')

cursor2 = conn2.cursor()
cursor1 = conn1.cursor()
cursor0 = conn0.cursor()

for fn in filename:
    sql0= "select ServerIP,DBName,CON from sysdb3.dbo.t_DB_Config with(nolock) where ID in (select dbid from sysdb3.dbo.t_dtfileindex with(nolock) where filename like '%{fn}%')".format(fn=fn)
    #print(sql0)
    cursor0.execute(sql0)
    results = cursor0.fetchall()
    db = results[0][1]
    #db = 'DT_TestData_LTE_1'
    #print(db)
    
    if db == 'DT_TestData_LTE_1':
        cursor = cursor1
    if db == 'DT_TestData_LTE_2':
        cursor = cursor2 

    sql = "select * from {db}.dbo.t_DTFileIndex with(nolock) where filename='{fn}'".format(db=db, fn=fn)
    
    cursor.execute(sql)
    results = cursor.fetchall()
    fn = results[0][1]
    id = results[0][0]
    begintime = results[0][7].strftime("%Y%m%d")
    # print(begintime)
    print(str(fn) + '\t' + str(id))
     
    res_sql ="""
    select 
    '{fn}' filename,
    a.ID, 
    a.FileID, 
    a.TestPointID, 
    a.DateTime, 
    a.HandsetDateTime, 
    a.NetTech, 
    a.Longitude, 
    a.Latitude, 
    a.Altitude, 
    a.Heading, 
    a.Speed, 
    a.MsgID, 
    a.Category, 
    a.MsgTitle, 
    a.MsgBinary, 
    a.Uplink, 
    a.Distance, 
    a.NRDeployType, 
    a.NRCAState,
    b.SS_RSRP,
    b.SS_RSRQ,
    b.SS_SINR,
    b.PUCCH_TxPower,
    b.PUSCH_Power,
    b.SRS_TxPower,
    b.PreambleTxPower,
    c.Pathloss,
    d.MCSAvg MCSAvg_DL,
    d.MCSBest MCSBest_DL,
    d.MCSMost MCSMost_DL,
    e.MCSAvg MCSAvg_UL,
    e.MCSBest MCSBest_UL,
    e.MCSMost MCSMost_UL,
    f.PDCPThr_DL,
    f.PDCPThr_UL,
    f.RLCThr_DL,
    f.RLCThr_UL,
    f.MACThr_DL,
    f.MACThr_UL,
    f.PhyThr_DL,
    f.PhyThr_UL,
    g.PDSCH_Slots,
    g.Num_PDSCH_Slot,
    g.PUSCH_Slots,
    g.Num_PUSCH_Slot,
    g.DL_Grants,
    g.Num_DL_Grant,
    g.UL_Grants,
    g.Num_UL_Grant,
    h.CQI_Avg,
    h.CQI_Best,
    h.CQI_Most,
    i.PDSCH_BLER,
    i.PDSCH_iBLER,
    i.PDSCH_rBLER,
    i.PDSCH_Total_Crcfailtb_Cnt,
    i.PDSCH_Initial_Crcfailtb_Cnt,
    i.PDSCH_Residual_Crcfailtb_Cnt,
    i.PUSCH_BLER,
    i.PUSCH_iBLER,
    i.PUSCH_rBLER,
    i.PUSCH_Total_Crcfailtb_Cnt,
    i.PUSCH_Initial_Crcfailtb_Cnt,
    i.PUSCH_Residual_Crcfailtb_Cnt
    from
    (
    select * from {db}.dbo.t_LTE_CommonParam_{begintime} where fileid={id} 
    
    ) a 
    left join
    (
    select * from NR_MeasurementInfo_{begintime} where fileid={id}
    ) b
    on a.testpointid = b.testpointid
    left join
    (
    select * from NR_ServingCellInfo_{begintime} where fileid={id}
    ) c
    on a.testpointid = c.testpointid
    left join
    (
    select * from NR_MCSStatInfoDL_{begintime} where fileid={id}
    ) d
    on a.testpointid = d.testpointid
    left join
    (
    select * from NR_MCSStatInfoUL_{begintime} where fileid={id}
    ) e
    on a.testpointid = e.testpointid
    left join
    (
    select * from NR_DataQoSParams_{begintime} where fileid={id}
    ) f
    on a.testpointid = f.testpointid
    left join
    (
    select * from NR_ResourceScheduleInfo_{begintime} where fileid={id}
    ) g
    on a.testpointid = g.testpointid
    left join
    (
    select * from NR_CQIStatsInfo_{begintime} where fileid={id}
    ) h
    on a.testpointid = h.testpointid
    left join
    (
    select * from NR_HARQInfo_{begintime} where fileid={id}
    ) i
    on a.testpointid = i.testpointid
    """.format(fn=fn,db=db,id=id,begintime=begintime)
    # print(res_sql)

    cursor.execute(res_sql)
    res = cursor.fetchall()

    #file_csv = codecs.open('normal/' + str(id) + '.csv', 'w+', 'utf-8')
    #writer = csv.writer(file_csv, delimiter=' ', quotechar=' ', quoting=csv.QUOTE_MINIMAL)
    title = ('filename','ID','FileID','TestPointID','DateTime','HandsetDateTime','NetTech','Longitude','Latitude','Altitude','Heading','Speed','MsgID','Category','MsgTitle','MsgBinary','Uplink','Distance','NRDeployType','NRCAState','SS_RSRP','SS_RSRQ','SS_SINR','PUCCH_TxPower','PUSCH_Power','SRS_TxPower','PreambleTxPower','Pathloss','MCSAvg_DL','MCSBest_DL','MCSMost_DL','MCSAvg_UL','MCSBest_UL','MCSMost_UL','PDCPThr_DL','PDCPThr_UL','RLCThr_DL','RLCThr_UL','MACThr_DL','MACThr_UL','PhyThr_DL','PhyThr_UL','PDSCH_Slots','Num_PDSCH_Slot','PUSCH_Slots','Num_PUSCH_Slot','DL_Grants','Num_DL_Grant','UL_Grants','Num_UL_Grant','CQI_Avg','CQI_Best','CQI_Most','PDSCH_BLER','PDSCH_iBLER','PDSCH_rBLER','PDSCH_Total_Crcfailtb_Cnt','PDSCH_Initial_Crcfailtb_Cnt','PDSCH_Residual_Crcfailtb_Cnt','PUSCH_BLER','PUSCH_iBLER','PUSCH_rBLER','PUSCH_Total_Crcfailtb_Cnt','PUSCH_Initial_Crcfailtb_Cnt','PUSCH_Residual_Crcfailtb_Cnt')
    #writer.writerow(title)
    #for row in res:
    #    writer.writerow(row)

    t_f = ''
    for field in title:
        t_f += str(field) + ','
    
    import os
    writer = open('normal/' + str(id) + '.csv', 'w')
    writer.writelines(t_f[:-1])
    writer.write('\n')

    for row in res:
        r_f = ''
        for field in row:
            r_f += str(field) + ','
        writer.writelines(r_f[:-1])
        writer.write('\n')

    writer.close()



conn0.close()
conn1.close()
conn2.close()
