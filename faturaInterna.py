#!/usr/bin/env python
"""
migPagCartao     : Processo de geração dp arquivo de statement do Will
Author           : Flavio Serafim de lima <flavio.lima@fastpays.com>
Data da Criacao  : 30/07/2024
License          : GPL
"""
import cx_Oracle
import sys
sys.path.append("/cms/cmsissr/CMS/PC/script/Python/lib")
import pAES
#import csv
#import gzip
import getpass
#import shutil
from decimal import Decimal
from datetime import datetime,timedelta
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser
inicio = datetime.now()

#Geracao de log do processamento.
print('INICINADO O PROCESSO DE GERACAO DO ARQUIVO DE FATURA DO WILL...')
print('VAMOS LA...')

def caregaVariaveis(Carrega):
    """[Funcao para buscar os parametro no aruqivo de configuracao]
    Args:
        bin_arquivo ([str]): [Nome do Carrega de Origem/Destino]
    Returns:
        [lista]: [retonar os parametro do arquivo de configuracao]
    """
    config = configparser.RawConfigParser()
    configfile="/cms/cmsissr/CMS/PC/script/Python/cfg/Config.cfg"
    config.read(configfile) 
    if Carrega == 'ORACLE':
        try:
            usuario  = config.get(Carrega, 'DB_USER')
            senha    = config.get(Carrega, 'DB_PASS')
            host     = config.get(Carrega, 'DB_HOST')
            port     = config.get(Carrega, 'DB_PORT')
            db       = config.get(Carrega, 'DB_BASE')
        except(configparser.Error, UnicodeDecodeError):
            print('ERRO : Erro ao obter os parametro do arquivo de configuracao')
            sys.exit( 3 )
        return {'USER':usuario, 'PASS':senha, 'HOST':host, 'PORT':port, 'BASE':db}
    elif Carrega == 'MOV_WILL':
        try:
            saida    = config.get(Carrega, 'DIR_OUT')
        except(configparser.Error, UnicodeDecodeError):
            print('ERRO : Erro ao obter os parametro do arquivo de configuracao')
            sys.exit( 3 )
        return {'SAIDA':saida}  

def calcular_cet_anual_com_iof(taxa_juros_mensal, iof_fixo, iof_diario, dias):
    """
    Calcula o CET anual incluindo o IOF, com base na taxa de juros mensal.
    :param taxa_juros_mensal: Taxa de juros mensal em porcentagem
    :param iof_fixo: IOF fixo aplicado ao valor total da operacoao (em porcentagem)
    :param iof_diario: IOF diario aplicado ao saldo devedor (em porcentagem)
    :param dias: numero de dias considerados para o cÃ¡lculo do IOF diario
    :return: CET anual em porcentagem
    """
    # Converte as taxas para decimais
    taxa_juros_mensal_decimal = taxa_juros_mensal / 100
    iof_fixo_decimal = iof_fixo / 100
    iof_diario_decimal = iof_diario / 100

    # Calcula o IOF total para o periodo
    total_iof = iof_fixo_decimal + (iof_diario_decimal * dias)

    # Calcula a taxa efetiva mensal incluindo IOF
    taxa_efetiva_mensal = taxa_juros_mensal_decimal + total_iof

    # Calcula o CET mensal
    cet_mensal=round((taxa_efetiva_mensal * 100),2)

    # Calcula o CET anual usando a formula dos juros compostos
    cet_anual = round (( ((1 + taxa_efetiva_mensal) ** 12 - 1) * 100),2)
    return cet_mensal,cet_anual

def main(entrada):
    """[Funcao principal do programa]
    Args:
        entrada  (str): [Nome do arquivo]
    Returns:
    """
    #VARIAVEIS 
    v_contador  = 0
    v_Oracle    = caregaVariaveis("ORACLE")
    v_Diretorio = caregaVariaveis("MOV_WILL")
    v_chave     = 'C0F35722B470A2649334E32BD42DD5B6'
    v_cuenta_ant= 0
    v_count_mov =1
    v_deb_nasc_conta = 0
    v_cred_nasc_conta = 0
    v_deb_inter_conta = 0
    v_cred_inter_conta = 0
    v_deb_nasc=0
    v_cred_nasc=0
    v_deb_inter=0
    v_cred_inter=0
    v_total_linhas=0
    t_tarifa = []

    if entrada == 'Encrypt':
        try:
            textSenha = getpass.getpass('DIGITE A SENHA DO BANCO DE DADOS:')

            # Calculo do Padding 
            if len(textSenha) % 16 == 0:
                pad = (len(textSenha))
            else:
                pad = ((((len(textSenha)) // 16) + 1)*16)    

            textSenha = (textSenha.ljust(pad," "))

            piroca=pAES.pAES()

            v_senha = piroca.pEncrypt(v_chave,textSenha)
            print('A senha encriptada eh: '+v_senha)
        except :
            print("Erro na Criptografia da senha do banco")

    else:

        try:
            piroca=pAES.pAES()
            v_senha = (piroca.pDecrypt(v_chave,v_Oracle['PASS'])).rstrip()
        except :
            print("Erro na Criptografia da senha do banco")

        v_arquivo=(v_Diretorio['SAIDA']+'STATEMENT_0021_31_C_20240728_'+entrada+'.OUT')
        v_arquivo=('./STATEMENT_0021_31_C_20240728_'+entrada+'.OUT')
        with open(v_arquivo, 'w') as file:
            file.write('0202407299645531STANDARD MASTERCARD           '+'\n')
            v_total_linhas+=1

            try:
                dsn_tns = cx_Oracle.makedsn(v_Oracle['HOST'],v_Oracle['PORT'], service_name=v_Oracle['BASE']) 
                connOracle = cx_Oracle.connect (user=v_Oracle['USER'], password=v_senha, dsn = dsn_tns) 
                cur = connOracle.cursor()

                # Buscar os dados do brasil configurado no sistema.
                print("INICIO              :",str(datetime.now().strftime("%H:%M:%S.%f")))
                print("CARREGANDO VARIAVEIS.")
                v_query ="""SELECT COD_ESTADO,DESCRIPCION FROM ISSR.ESTADO"""
                cur.execute(v_query)
                cur_estados=cur.fetchall()
                print("ESTADO              :",str(datetime.now().strftime("%H:%M:%S.%f")))

                # Buscar os paises configurados de sistema.
                v_query ="""SELECT PAIS,DESCRIPCION FROM ISSR.PAISES"""
                cur.execute(v_query)
                cur_paises=cur.fetchall()
                print("PAISES              :",str(datetime.now().strftime("%H:%M:%S.%f")))

                # Buscar as rubricas configuradas no sistema.
                v_query ="""SELECT RUBRO, TIPO_DE_RUBRO, DESCRIPCION FROM RUBROS"""
                cur.execute(v_query)
                cur_rubro=cur.fetchall()
                print("RUBRICA             :",str(datetime.now().strftime("%H:%M:%S.%f")))

                # Buscar as moedas configuradas no sistema.
                v_query ="""SELECT CODIGO_DE_MONEDA, SIMBOLO FROM MONEDAS ORDER BY 1 DESC"""
                cur.execute(v_query)
                cur_moeda=cur.fetchall()
                print("MOEDA               :",str(datetime.now().strftime("%H:%M:%S.%f")))

                # Buscar os tipos de cartões do Will.
                v_query ="""SELECT TIPO_TARJETA, DESCRIPCION FROM ISSR.TIPOS_TARJETAS WHERE EMISOR = 21 AND PRODUCTO = 31"""
                cur.execute(v_query)
                cur_tipo_pan=cur.fetchall()
                print("TIPO CARTAO         :",str(datetime.now().strftime("%H:%M:%S.%f")))

                # Buscar taxas e tarifas do Will;
                v_query ="""SELECT AFFINITY_GROUP,
                               MAX(CASE WHEN RUBRIC = 13 THEN PROD_FINAN_ML ELSE 0 END) AS ENCARGOS,
                               MAX(CASE WHEN RUBRIC = 16 THEN PROD_FINAN_ML ELSE 0 END) AS MULTA,
                               MAX(CASE WHEN RUBRIC = 30 THEN PROD_FINAN_ML ELSE 0 END) AS JUROS
                              FROM FINAN_INT_RATES_ISSR_PRO_CT
                                WHERE ISSUER = 21
                                AND PRODUCT = 31
                                AND PROCESS_TYPE = 'M' 
                                AND CARD_TYPE = 1
                                GROUP BY AFFINITY_GROUP
                                ORDER BY AFFINITY_GROUP DESC"""
                cur.execute(v_query)
                cur_tarifas=cur.fetchall()
                iof_fixo=0.38
                iof_diario=0.0082
                dias = 30

                for ga,encargo,multa,juros in cur_tarifas:
                    #print(calcular_cet_anual_com_iof(produc_fian, iof_fixo, iof_diario, dias))
                    v_cet = calcular_cet_anual_com_iof(encargo, iof_fixo, iof_diario, dias)
                    t_tarifa.append((ga,Decimal(str(encargo)),Decimal(str(multa)),Decimal(str(juros)),Decimal(str(v_cet[0])),Decimal(str(v_cet[1]))))
                print("TAXAS TARIFAS       :",str(datetime.now().strftime("%H:%M:%S.%f")))

                #Dados do cierre que esta cortando no dia.
                v_query ="""SELECT EMISOR, PRODUCTO, CIERRE, FECHA_CIERRE, FECHA_VENCIMIENTO, FECHA_CIERRE_ANT, FECHA_VENCIMIENTO_ANT, PERIODO_CIERRE 
                            FROM ISSR.VARIABLES_ENTORNO
                            WHERE EMISOR   = 21
                            AND PRODUCTO = 31
                            AND FECHA_CIERRE =20240628"""
                cur.execute(v_query)
                cur_variable_entorno=cur.fetchone()
                print("VARIABLES ENTORNO   :",str(datetime.now().strftime("%H:%M:%S.%f")))

                #Calculo do proximo perodo
                v_perido_date=datetime.strptime(str(cur_variable_entorno[7]),'%Y%m')
                v_prox_periodo=(v_perido_date + timedelta(days=31)).strftime('%Y%m')

                # buscar a data de fechamento e do proximo periodo.
                v_query ="""SELECT FECHA_CIERRE, FECHA_VENCIMIENTO 
                             FROM ISSR.CALENDARIO_CIERRES
                            WHERE EMISOR = """+str(cur_variable_entorno[0])+"""
                             AND PRODUCTO = """+str(cur_variable_entorno[1])+"""
                             AND CIERRE = """+str(cur_variable_entorno[2])+"""
                             AND PERIODO = """+v_prox_periodo+""
                cur.execute(v_query)
                cur_prox_fatura=cur.fetchone()
                print("CALENDARIO CIERRES  :",str(datetime.now().strftime("%H:%M:%S.%f")))

                #Query que busta as contas para atualizar
                sql_conta="""SELECT C.EMISOR,
                            C.SUCURSAL_EMISOR,
                            C.PRODUCTO,
                            C.NUMERO_CUENTA,
                            C.TIPO_DE_DOCUMENTO,
                            C.DOCUMENTO,
                            C.GRUPO_AFINIDAD,
                            C.ESTADO,
                            C.CIERRE,
                            C.LIMITE_CREDITO,
                            C.SALDO_CIERRE_ML,
                            C.PAGO_MINIMO_ML,
                            SC.PERIODO_CIERRE,
                            SC.PERIODO_CIERRE_ANT
                        FROM ISSR.CUENTAS                 C,
                            ISSR.SALDOS_CUENTAS_EMISION  SE,
                            ISSR.SPLIT_CUENTAS_MENSUAL   SC
                        WHERE C.EMISOR = SE.EMISOR
                        AND C.SUCURSAL_EMISOR = SE.SUCURSAL_EMISOR
                        AND C.PRODUCTO = SE.PRODUCTO
                        AND C.NUMERO_CUENTA = SE.NUMERO_CUENTA
                        AND C.CIERRE = SE.CIERRE
                        AND SC.EMISOR = SE.EMISOR
                        AND SC.SUCURSAL_EMISOR = SE.SUCURSAL_EMISOR
                        AND SC.PRODUCTO = SE.PRODUCTO
                        AND SC.NUMERO_CUENTA = SE.NUMERO_CUENTA
                        AND SC.PERIODO_CIERRE = SE.PERIODO_CIERRE
                        AND C.EMISOR = 21
                        AND C.SUCURSAL_EMISOR = 1
                        AND C.PRODUCTO = 31
                        AND C.CIERRE = 25
                        AND SE.MOTIVO = 0
                        --AND C.NUMERO_CUENTA in (7416789,7161262)
                        AND ROWNUM < 545
                        AND SC.NRO_PROCESO = 10"""
                
                print('RODANDO O CURSOR PRINCIPAL...')
                cur.execute(sql_conta)
                cur_principal=cur.fetchall()
                print("PRINCIPAL           :",str(datetime.now().strftime("%H:%M:%S.%f")))

                print('INICIANDO A GERACAO DO ARQUIVO.')
                for res in cur_principal:
                    v_emisor = res[0]
                    v_sucursal = res[1]
                    v_produto = res[2]
                    v_cuenta = res[3]
                    v_tipo_doc = res[4]
                    v_doc = str(res[5])
                    v_cierre = res[8]
                    v_periodo = res[12]

                    if v_cuenta_ant != 0:
                        v_deb_nasc_conta = v_deb_nasc_conta+v_deb_nasc
                        v_cred_nasc_conta = v_cred_nasc_conta+v_cred_nasc
                        v_deb_inter_conta = v_deb_inter_conta+v_deb_inter
                        v_cred_inter_conta = v_cred_inter_conta+v_cred_inter

                        file.write('5'+
                                v_cartao+
                                v_nome_embossing+
                                (str(v_deb_nasc).replace('.','')+'00').zfill(18)+
                                (str(v_cred_nasc).replace('.','')+'00').zfill(18)+
                                (str(v_deb_inter).replace('.','')+'00').zfill(18)+
                                (str(v_cred_inter).replace('.','')+'00').zfill(18)+
                                '\n')
                        v_total_linhas+=1   
                        v_deb_nasc=0
                        v_cred_nasc=0
                        v_deb_inter=0
                        v_cred_inter=0

                        file.write('6'+
                                (str(v_deb_nasc_conta).replace('.','')+'00').zfill(18)+
                                (str(v_cred_nasc_conta).replace('.','')+'00').zfill(18)+
                                (str(v_deb_inter_conta).replace('.','')+'00').zfill(18)+
                                (str(v_cred_inter_conta).replace('.','')+'00').zfill(18)+
                                str(v_count_mov).zfill(8)+
                                '\n')    
                        v_total_linhas+=1                      
                        v_deb_nasc_conta = 0
                        v_cred_nasc_conta = 0
                        v_deb_inter_conta = 0
                        v_cred_inter_conta = 0
                        v_count_mov=1

                    v_cuenta_ant = v_cuenta
                    v_cartao = 0
                    #print(str(datetime.now().strftime("%H:%M:%S.%f")))
                        

                    v_query = """SELECT TARJETA 
                                   FROM TARJETAS T
                                  WHERE T.EMISOR = """+str(v_emisor)+"""
                                    AND T.SUCURSAL_EMISOR = """+str(v_sucursal)+"""
                                    AND T.PRODUCTO = """+str(v_produto)+"""
                                    AND T.NUMERO_CUENTA = """+str(v_cuenta)+"""
                                    AND T.TIPO_DE_DOCUMENTO = """+str(v_tipo_doc)+"""
                                    AND T.DOCUMENTO = '"""+str(v_doc)+"""'
                                    AND T.CORRELATIVO = (SELECT MAX(TA.CORRELATIVO)
                                                           FROM TARJETAS TA
                                                          WHERE T.EMISOR = TA.EMISOR
                                                            AND T.SUCURSAL_EMISOR = TA.SUCURSAL_EMISOR
                                                            AND T.PRODUCTO = TA.PRODUCTO
                                                            AND T.NUMERO_CUENTA = TA.NUMERO_CUENTA
                                                            AND T.TIPO_DE_DOCUMENTO = TA.TIPO_DE_DOCUMENTO
                                                            AND TA.ADICIONAL = 'N'
                                                            AND T.DOCUMENTO = TA.DOCUMENTO)"""

                    cur.execute(v_query)
                    cur_pan=cur.fetchone()

                    v_query = """SELECT NOMBRE 
                                   FROM PERSONAS P
                                  WHERE P.EMISOR = """+str(v_emisor)+"""
                                    AND P.SUCURSAL_EMISOR = """+str(v_sucursal)+"""
                                    AND P.DOCUMENTO = '"""+str(v_doc)+"""'
                                    AND P.TIPO_DE_DOCUMENTO = """+str(v_tipo_doc)+""

                    cur.execute(v_query)
                    cur_nome=cur.fetchone()
                    #print(cur_nome[:60])
                    #print("NOME                :",str(datetime.now().strftime("%H:%M:%S.%f")))

                    v_query = """SELECT CALLE, NUMERO_PUERTA, BARRIO, COMPLEMENTO, CIUDAD, COD_ESTADO, PAIS, CODIGO_POSTAL, E_MAIL
                                   FROM ISSR.DIR_VINCULADOS_MAX D 
                                  WHERE D.EMISOR = """+str(v_emisor)+"""
                                    AND D.SUCURSAL_EMISOR = """+str(v_sucursal)+"""
                                    AND D.PRODUCTO = """+str(v_produto)+"""
                                    AND D.NUMERO_CUENTA = """+str(v_cuenta)+"""
                                    AND D.TIPO_DE_DOCUMENTO = """+str(v_tipo_doc)+"""
                                    AND D.DOCUMENTO = '"""+str(v_doc)+"""'
                                    AND D.TIPO_DIRECCION = 1"""

                    cur.execute(v_query)
                    #print(v_query)
                    cur_endereco=cur.fetchall()
                    #print("ENDERECO            :",str(datetime.now().strftime("%H:%M:%S.%f")))

                    for codigo,sigla in cur_estados:
                        if codigo == cur_endereco[0][5]:
                            v_estado = sigla.strip()
                            break

                    for codigo,paises in cur_paises:
                        if codigo == cur_endereco[0][6]:
                            v_paises = paises.strip().ljust(30)
                            break

                    v_query = """SELECT ID_MOVTOS_CUENTAS, EMISOR, SUCURSAL_EMISOR, PRODUCTO, NUMERO_CUENTA, CORRELATIVO, TARJETA, FECHA_VALOR, FECHA_MOVIMIENTO, ID_COMERCIO_EMI, CODIGO_OPERACION, RUBRO, ID_TRANSACCION, IMPORTE_ML, VAN, SON, ID_LINEA, MONEDA_ORIGEN, IMPORTE_ORIGEN, ID_MOVTOS_ORIGEN
                                FROM ISSR.MOVTOS_CUENTAS M
                                WHERE M.EMISOR = """+str(v_emisor)+"""
                                AND M.SUCURSAL_EMISOR = """+str(v_sucursal)+"""
                                AND M.PRODUCTO = """+str(v_produto)+"""
                                AND M.NUMERO_CUENTA = """+str(v_cuenta)+"""  
                                AND M.CIERRE = """+str(v_cierre)+"""   
                                AND M.PERIODO_CIERRE = """+str(v_periodo)+"""
                                ORDER BY CORRELATIVO,ID_MOVTOS_CUENTAS"""
                    cur.execute(v_query)
                    cur_mov=cur.fetchall()

                    v_query="""SELECT VLR_AVAILABLE_BUY FROM TABLE(ISSR.FUNC_AVAILABLE_PER_LINE("""+str(v_emisor)+""","""+str(v_sucursal)+""","""+str(v_produto)+""","""+str(v_cuenta)+""",'ROOT'))"""
                    cur.execute(v_query)
                    cur_disponivel=cur.fetchone()

                    if int(cur_disponivel[0]) > 0:
                        v_disponivel = (str(int(cur_disponivel[0]*10000)).replace('.','')).zfill(18)
                    else:
                        v_disponivel = '000000000000000000'

                    file.write('1'+
                               str(res[3]).rjust(10)+
                               str(res[6]).zfill(7)+
                               cur_pan[0]+
                               str(res[5]).ljust(15)+
                               cur_nome[0].strip().ljust(60)+
                               (cur_endereco[0][0].strip()+' '+cur_endereco[0][1].strip()).ljust(65)+
                               cur_endereco[0][3][:40]+
                               cur_endereco[0][2][:50]+
                               cur_endereco[0][4][:50]+
                               v_estado+
                               v_paises+
                               cur_endereco[0][7].ljust(10)+
                               cur_endereco[0][8]+
                               str(cur_variable_entorno[3])+
                               str(cur_variable_entorno[4])+
                               str(cur_variable_entorno[5])+
                               str(cur_variable_entorno[6])+
                               str(cur_prox_fatura[0])+
                               str(cur_prox_fatura[1])+
                               (str(int(res[9]*10000)).replace('.','')).zfill(18)+
                               v_disponivel+
                               '\n')
                    v_total_linhas+=1

                    for ga,encargo,multa,juros,cet_mensal,set_anual in t_tarifa:
                        if ga == res[6]:
                            v_tarifa=(ga,encargo,multa,juros,cet_mensal,set_anual) 
                            break

                        #t_tarifa.append((ga,rubrica,produc_fian,v_cet[0],v_cet[1]))
                    file.write('2'+
                               (str(int(res[10]*10000)).replace('.','')).zfill(18)+
                               (str(int(res[11]*10000)).replace('.','')).zfill(18)+
                               (str(int(v_tarifa[1]*100)).replace('.','')).zfill(8)+
                               (str(int(v_tarifa[2]*100)).replace('.','')).zfill(8)+
                               (str(int(v_tarifa[3]*100)).replace('.','')).zfill(8)+
                               (str(int(v_tarifa[4]*100)).replace('.','')).zfill(8)+
                               (str(int(v_tarifa[5]*100)).replace('.','')).zfill(8)+
                               '\n')



                    for mov in cur_mov:
                        #print(mov)
                        v_count_mov+=1
                        for cod_rubro,tipo_rubro,desc_rubro in cur_rubro:
                            if cod_rubro == mov[11]:
                                v_rubro = desc_rubro[:30].ljust(30)
                                break

                        for cod_moeda,desc_moeda in cur_moeda:
                            if cod_moeda == mov[17]:
                                v_moeda = desc_moeda.strip().ljust(3)
                                break

                        if mov[12] > 0 and mov[19] == 0:
                            v_query ="""SELECT LOCALIDAD_COMERCIO,MCC,IMPORTE_MR FROM ISSR.TRANS_BATCH_ISSR TB WHERE TB.ID_TRANSACCION = """+str(mov[12])+""
                            cur.execute(v_query)
                            cur_trans_batch=cur.fetchone()
                            v_cidade_est=cur_trans_batch[0]
                            v_mcc=cur_trans_batch[1]
                            v_moeda_ref=cur_trans_batch[2]

                            v_query ="""SELECT ID_TRANS_ISO_ISSR FROM ISSR.MOVTOS_AUTH WHERE ID_MOVTOS_CUENTAS ="""+str(mov[0])+""
                            cur.execute(v_query)
                            cur_trans_iso=cur.fetchone()

                            if cur_trans_iso is None:
                                v_id_trans_iso='0'
                            else:
                                v_id_trans_iso=cur_trans_iso[0]

                        else:
                            v_cidade_est= ' '*13
                            v_mcc= '0000'
                            v_moeda_ref = '0000000000000000'
                            v_id_trans_iso='0'

                        if mov[10] == 1:
                            v_operador='-'
                        else:
                            v_operador='+'

                        if mov[17] == 986:
                            v_moeda_ori=int(mov[13]*10000)
                        else:
                            v_moeda_ori=int(mov[18]*10000)

                        if mov[6] != v_cartao and v_cartao != 0:
                            v_deb_nasc_conta = v_deb_nasc_conta+v_deb_nasc
                            v_cred_nasc_conta = v_cred_nasc_conta+v_cred_nasc
                            v_deb_inter_conta = v_deb_inter_conta+v_deb_inter
                            v_cred_inter_conta = v_cred_inter_conta+v_cred_inter            

                            file.write('5'+
                                    v_cartao+
                                    cur_pan_mov[0].ljust(30)+
                                    (str(v_deb_nasc).replace('.','')+'00').zfill(18)+
                                    (str(v_cred_nasc).replace('.','')+'00').zfill(18)+
                                    (str(v_deb_inter).replace('.','')+'00').zfill(18)+
                                    (str(v_cred_inter).replace('.','')+'00').zfill(18)+
                                   '\n')
                            v_total_linhas+=1   
                            v_deb_nasc=0
                            v_cred_nasc=0
                            v_deb_inter=0
                            v_cred_inter=0

                        # Header do cartao.
                        if mov[6] != v_cartao:
                            v_cartao = mov[6]
                            v_query="""SELECT NOMBRE_EMBOSADO,TIPO_TARJETA FROM ISSR.TARJETAS WHERE TARJETA = '"""+mov[6]+"'"
                            cur.execute(v_query)
                            cur_pan_mov=cur.fetchone()

                            for codigo,desc_tipo in cur_tipo_pan:
                                if codigo == cur_pan_mov[1]:
                                    v_desc_tipo_pan = desc_tipo.strip().ljust(30)
                                    break                            

                            v_nome_embossing=cur_pan_mov[0].ljust(30)
                            v_cod_tipo_pan=str(cur_pan_mov[1]).zfill(2)

                            file.write('3'+
                                    mov[6]+
                                    v_nome_embossing+
                                    v_cod_tipo_pan+
                                    v_desc_tipo_pan+
                                   '\n')
                            v_total_linhas+=1

                        if (mov[17] == 986) and (mov[10] == 0):
                            v_deb_nasc = v_deb_nasc+int(mov[13])
                        elif (mov[17] == 986) and (mov[10] == 1):
                            v_cred_nasc = v_cred_nasc+int(mov[13])
                        elif (mov[17] != 986) and (mov[10] == 0):
                            v_deb_inter = v_deb_inter+int(mov[13])
                        elif (mov[17] != 986) and (mov[10] == 1):
                            v_cred_inter = v_cred_inter+int(mov[13])


                        file.write('4'+
                                   str(mov[8])+
                                   str(mov[7])+
                                   mov[9].ljust(36)+
                                   v_rubro+
                                   v_cidade_est[:13].ljust(13)+
                                   (str(v_moeda_ori).replace('.','')).zfill(18)+
                                   v_operador+
                                   v_moeda+
                                   (str(mov[13]).replace('.','')+'00').zfill(18)+
                                   v_operador+
                                   (str(v_moeda_ref).replace('.','')+'00').zfill(18)+
                                   ' '+
                                   str(mov[11]).zfill(4)+
                                   str(mov[14]).zfill(2)+
                                   '/'+
                                   str(mov[15]).zfill(2)+
                                   ' '+
                                   '000000000000000000'+ # cotacao
                                   str(v_mcc)+
                                   str(mov[12]).zfill(18)+
                                   str(v_id_trans_iso).zfill(18) + # cotacao
                                   str(mov[0]).zfill(18)+
                                   '\n')
                        v_total_linhas+=1

                    # contador de linhas
                    if (v_contador % 100 == 0):
                        print ("Linhas Processadas :", v_contador, str(datetime.now().strftime("%H:%M:%S.%f")))
                        v_contador=v_contador+1
                    else:
                        v_contador=v_contador+1 
                        
                file.write('T'+
                        str(v_contador).zfill(18)+
                        str(v_total_linhas+1).zfill(18)+
                        '\n')

            except cx_Oracle.DatabaseError as e:
                error, = e.args
                if error.code == 955:
                    print('Table already exists')
                if error.code == 1:
                    print('Cartao ja incluido')
                if error.code == 933:
                    print('Query errada')
                if error.code == 1031:
                    print("Insufficient privileges - are you sure you're using the owner account?")
                print("Codigo de erro ORA-",error.code)
                print(error.message)
                print(error.context)
                print("ERRO : Problema na funcao conecta_oracle")

                fim=datetime.now()      #Data FInal do processamento.
                print ("\n")            #Saida retorno do programa caso seja finalizado corretamente.
                print ("The program concludes Error")
                print ("Processed lines        ", v_contador)
                print ("Start time             ", inicio.strftime('%H:%M:%S'))
                print ("Finish time            ", fim.strftime('%H:%M:%S'))
                sys.exit(1)

        fim=datetime.now()      #Data FIn1al do processamento.
        print ("The program concludes correctly")
        print ("File Name              ", v_arquivo)
        print ("Processed lines        ", v_contador)
        print ("Start time             ", inicio.strftime('%H:%M:%S'))
        print ("Finish time            ", fim.strftime('%H:%M:%S'))

# Main do Programa (De onde o Programa vai Iniciar)
if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
