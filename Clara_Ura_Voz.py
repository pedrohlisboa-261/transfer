# -*- coding: utf-8 -*-
import cx_Oracle
from pymongo import errors
from datetime import datetime
from datetime import timedelta
import Conexao_Mongo
import flask
from flask import request, jsonify, render_template
import json
import os

#cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_21_3")

app = flask.Flask(__name__)
app.config["DEBUG"] = True

class Relatorio_Ura_Voz:
    def __init__(self, empresa,contaContrato,sessao,nome,sobrenome,nomeMae,dataNascimento,numeroRg,numeroCpf): # , ini, fim , had_problem
        self.empresa = empresa
        self.contaContrato = contaContrato
        self.sessao = sessao
        self.nome = nome
        self.sobrenome = sobrenome
        self.nomeMae = nomeMae
        self.dataNascimento = dataNascimento
        self.numeroRg = numeroRg
        self.numeroCpf = numeroCpf

        
        
    def toTuple(self):
        msg =(
            self.empresa or None,
            self.contaContrato or None,
            self.sessao or None,
            self.nome or None,
            self.sobrenome or None,
            self.nomeMae or None,
            self.dataNascimento or None,
            self.numeroRg or None,
            self.numeroCpf or None
        )
        return msg
# FIM DA CLASSE RELATORIO URAS

        
#def insert_SQL(empresa, relatoriouras, descricao, data_inicio, ini, fim):
def insert_SQL(solicitacoes,descricao):
    try:

        insert_solicitacoes = []

        for solic in solicitacoes:
            insert_solicitacoes.append(solic.toTuple())

        #json_string = json.dumps(insert_solicitacoes)

        json_string = jsonify(

        empresa = solicitacoes[0].empresa,
        contaContrato = solicitacoes[0].contaContrato,
        sessao = solicitacoes[0].sessao,
        nome = solicitacoes[0].nome,
        sobrenome = solicitacoes[0].sobrenome,
        nomeMae = solicitacoes[0].nomeMae,
        dataNascimento = solicitacoes[0].dataNascimento,
        numeroRg = solicitacoes[0].numeroRg,
        numeroCpf = solicitacoes[0].numeroCpf
        )

        return json_string

    except:
        status_code = 500
        response = {
            'error': {
                'type': 'Erro ao achar número',
                'message': 'Conversa não localizada'
            }
        }
        return jsonify(response),status_code
                    
#def executeQueryUras(empresa, descricao, data_inicio, ini, fim):
def executeQueryUras(descricao, data_inicio, parametroConta):
    try:
        client = Conexao_Mongo.connectToClara("crm")    
        database = client["crm"]
        collection = database["mensagens"]
        
        
        data_mongo = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT03:00:00.000-00:00")
        
        
        pipeline_uras = [
            {u"$match": {
                      u"context.from": parametroConta,
                      u"dataTimezone": {
                          "$gte" : data_mongo
                      },
                      u"context.cpf_cnpj_cc": {
                          u"$gt": u"0"},
                    u"context.dadoClienteCliente":{u"$exists":u"true"}
                      }},
        {u"$group": {u"_id":
                      {u"context\u1390sessao": u"$context.sessao",
                       u"context\u1390cpf_cnpj_cc": u"$context.cpf_cnpj_cc",
                       u"context\u1390empresa": u"$context.empresa",
                       u"dataTimezone": u"$dataTimezone",
                       u"context\u1390dadoClienteCliente": u"$context.dadoClienteCliente",
                       u"context\u1390marcos": u"$context.marcos"}
                  }},
            {u"$project": {u"context.empresa": u"$_id.context\u1390empresa",
                           u"context.cpf_cnpj_cc": u"$_id.context\u1390cpf_cnpj_cc",
                           u"dataTimezone": u"$_id.dataTimezone",
                           u"context.sessao": u"$_id.context\u1390sessao",
                           u"context.marcos": u"$_id.context\u1390marcos",
                           u"context.dadoClienteCliente": u"$_id.context\u1390dadoClienteCliente",
                           u"_id": 0.0}},
        {u"$limit": 1},
        {u"$sort": {u"_id": -1}}
            ]

        
        cursor_relatoriouras = collection.aggregate(
            pipeline_uras, 
            allowDiskUse = True
        )
        
        ura = []
        
        for doc in cursor_relatoriouras:
            

            empresa = doc['context']['empresa'] or None

            sessao = doc['context']['sessao'] or None

            nome = doc['context']['dadoClienteCliente']['nome'] or None

            sobrenome = doc['context']['dadoClienteCliente']['sobrenome'] or None

            nomeMae = doc['context']['dadoClienteCliente']['nomeMae'] or None

            dataNascimento = doc['context']['dadoClienteCliente']['dataNascimento'] or None

            numeroRg = doc['context'][
                           'dadoClienteCliente']['numeroRg'] or None

            numeroCpf = doc['context']['dadoClienteCliente']['numeroCpf'] or None

            contaContrato = doc['context'][
                           'dadoClienteCliente']['contaContrato'] or None
            
            ura.append(Relatorio_Ura_Voz(empresa,contaContrato, sessao,nome,sobrenome,nomeMae,dataNascimento,numeroRg,numeroCpf))

    except (Exception, errors.PyMongoError) as error:
        
        data_fim = datetime.now()
        print(error)
        # Clara_ETL_Log.InsertLog(descricao, data_inicio, data_fim, 0, 1, error)
        
    finally:
        client.close()


    return ura

#def ExecutarETL(empresa, ini, fim):
def ExecutarETL(parametroConta):
       
    descricao = Relatorio_Ura_Voz.__name__
    
    data_inicio = datetime.now()
          
    relatoriouras = executeQueryUras(descricao, data_inicio,parametroConta)

    retorno = insert_SQL(relatoriouras,descricao)

    return retorno
#ExecutarETL("Cemar")

@app.route('/in', methods=['GET'])
def home():
    parametroConta = request.args.get('conta')
    relatorio = ExecutarETL(parametroConta)

    return relatorio

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
