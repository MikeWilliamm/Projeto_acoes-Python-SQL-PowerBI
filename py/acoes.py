import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import json 
import psycopg2
from sqlalchemy import create_engine

url = 'https://statusinvest.com.br/category/advancedsearchresult?search={"Sector":"","SubSector":"","Segment":"","my_range":"-20;100","forecast":{"upsideDownside":{"Item1":null,"Item2":null},"estimatesNumber":{"Item1":null,"Item2":null},"revisedUp":true,"revisedDown":true,"consensus":[]},"dy":{"Item1":null,"Item2":null},"p_L":{"Item1":null,"Item2":null},"peg_Ratio":{"Item1":null,"Item2":null},"p_VP":{"Item1":null,"Item2":null},"p_Ativo":{"Item1":null,"Item2":null},"margemBruta":{"Item1":null,"Item2":null},"margemEbit":{"Item1":null,"Item2":null},"margemLiquida":{"Item1":null,"Item2":null},"p_Ebit":{"Item1":null,"Item2":null},"eV_Ebit":{"Item1":null,"Item2":null},"dividaLiquidaEbit":{"Item1":null,"Item2":null},"dividaliquidaPatrimonioLiquido":{"Item1":null,"Item2":null},"p_SR":{"Item1":null,"Item2":null},"p_CapitalGiro":{"Item1":null,"Item2":null},"p_AtivoCirculante":{"Item1":null,"Item2":null},"roe":{"Item1":null,"Item2":null},"roic":{"Item1":null,"Item2":null},"roa":{"Item1":null,"Item2":null},"liquidezCorrente":{"Item1":null,"Item2":null},"pl_Ativo":{"Item1":null,"Item2":null},"passivo_Ativo":{"Item1":null,"Item2":null},"giroAtivos":{"Item1":null,"Item2":null},"receitas_Cagr5":{"Item1":null,"Item2":null},"lucros_Cagr5":{"Item1":null,"Item2":null},"liquidezMediaDiaria":{"Item1":null,"Item2":null},"vpa":{"Item1":null,"Item2":null},"lpa":{"Item1":null,"Item2":null},"valorMercado":{"Item1":null,"Item2":null}}&CategoryType=1'


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

dat = {'q':'goog'}
response = requests.get(url, headers=headers)
dados = json.loads(response.text)


df = pd.DataFrame(dados)
df.columns = df.columns.str.lower()

print('Web Scraper executado com sucesso!')

connection_data = psycopg2.connect(host = 'localhost', database = 'postgres', user = 'postgres', password = '123456', port = 5432) 
cur = connection_data.cursor()

table = '''
    CREATE TABLE IF NOT exists investimentos.acoes_cotacao(
	companyid int4 NULL,
	companyname varchar NULL,
	ticker varchar NULL,
	price float4 NULL,
	p_l float4 NULL,
	p_vp float4 NULL,
	p_ebit float4 NULL,
	p_ativo float4 NULL,
	ev_ebit float4 NULL,
	margembruta float4 NULL,
	margemebit float4 NULL,
	margemliquida float4 NULL,
	p_sr float4 NULL,
	p_capitalgiro float4 NULL,
	p_ativocirculante float4 NULL,
	giroativos float4 NULL,
	roe float4 NULL,
	roa float4 NULL,
	roic float4 NULL,
	dividaliquidaebit float4 NULL,
	pl_ativo float4 NULL,
	passivo_ativo float4 NULL,
	liquidezcorrente float4 NULL,
	peg_ratio float4 NULL,
	receitas_cagr5 float4 NULL,
	liquidezmediadiaria float4 NULL,
	vpa float4 NULL,
	lpa float4 NULL,
	valormercado float4 NULL,
	dividaliquidapatrimonioliquido float4 NULL,
	dy float4 NULL,
	lucros_cagr5 float4 NULL
);'''
cur.execute(table)
connection_data.commit()


truncate = '''truncate table investimentos.acoes_cotacao;'''
cur.execute(truncate)
connection_data.commit()

engine = create_engine('postgresql+psycopg2://postgres:123456@localhost:5432/postgres')
df.to_sql('acoes_cotacao', engine, schema='investimentos',index=False, if_exists='append')

print('Importação de dados de acoes finalizada!')

with open(r'C:\Users\Mike\Desktop\Projeto acoes\sql\rankings.sql', 'r') as arq:
	sql = arq.read()

cur.execute(sql)
connection_data.commit()
print('Tabela de rankings atualizada!')