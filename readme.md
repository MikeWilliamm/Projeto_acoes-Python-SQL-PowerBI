## Objetivo
Construir uma aplicação de dados com ações da bolsa de valores brasileira(B3), essa aplicação deve ser capaz de auxiliar na tomada de decisão de investidores. A aplicação deve conter rankings de melhores ações, esses rankings devem ser baseados nos conceitos de dois investidores fundamentalistas, Benjamin Graham e Joel Greenblatt, e um terceiro ranking que é a união da metodologia de Graham e Joel. Também para complementar as análises, utilizando a fórmula de Benjamin Graham, criar uma segunda dashboard que irá apresentar o valor intrínseco e a margem de segurança de cada ação selecionada.


## Resumo geral do projeto 
<b>Resumo:</b>
<p align="center">
  <img  src="prints/0_fluxo_de_processo.png">
  <b>Fluxo de processo</b>
</p>

Inicialmente a aplicação utiliza um WebScraper, os dados de indicadores de cada ação são extraídos do site [Status Invest](https://statusinvest.com.br/), após isso, os dados são armazenados em um banco de dados relacional, através da linguagem SQL é construído os rankings utilizando os conceitos dos dois investidores fundamentalistas, Benjamin Graham e Joel Greenblatt, e o terceiro ranking é a combinação que une as metodologias dos dois investidores. Com a metodologia dos rankings aplicada via SQL. Posteriormente, a ferramenta Power B.I consulta os dados de rankings e dados gerais de todos os indicadores de ações já prontos e tratados no banco de dados, a ferramenta Figma foi utilizada para criar o visual das dashboards, no Power B.I existe 3 dashboards, a primeira é a capa cuja responsabilidade é conter os botões de direcionamentos para todas as outras dashboards do projeto, acessando a dashboard chamada rankings fundamentalista, é possível visualizar os 3 diferentes rankings e uma tabela que contem todos os indicadores de ações, aqui é possível filtrar e pesquisar uma ou um grupo de ações para análise seu posicionamento em cada ranking e todos seus indicadores fundamentalistas, vale ressaltar que não são exibidas as ações com liquidez média diária maior que 1 milhão e com o preço sobre lucro e preço sobre valor patrimonial negativos. Acessando a dashboard chamada Margem de segurança, é possível visualizar o valor intrínseco e a margem de segurança de cada ação selecionada, esses valores são calculados com a  fórmula de Benjamin Graham VI= √ (22,5 x LPA x VPA).

## Video apresentação
[![Vídeo](https://raw.githubusercontent.com/MikeWilliamm/Projeto_acoes-Python_SQL_PowerBI/main/prints/6_capa_video.png)]https://raw.githubusercontent.com/MikeWilliamm/Projeto_acoes-Python-SQL-PowerBI/main/prints/6_capa_video.png)
## Solução proposta

<b>Stacks:</b> 
Python (requests, pandas, bs4, json, psycopg2, sqlalchemy)
PostgreSQL
Power B.I
Figma
google inpect 

A arquitetura da aplicação se inicia com um Web Scraper com a linguagem de programação Python, nessa etapa o programa faz uma requisição a mesma URL que o site [Status Invest](https://statusinvest.com.br/) faz para apresentar os dados no site, essa URL fornece todos os dados de indicadores de ações em formato <b>Json</b>, essa URL foi encontrada por análise no site com a ferramenta de inspeção google inpect: 
<p align="center">
  <img  src="prints/1_request_url.png">
  <b>URL com dados brutos</b>
</p>

Extração dos dados:
````
url = 'https://statusinvest.com.br/category/advancedsearchresult?search={"Sector":"","SubSector":"","Segment":"","my_range":"-20;100","forecast":{"upsideDownside":{"Item1":null,"Item2":null},"estimatesNumber":{"Item1":null,"Item2":null},"revisedUp":true,"revisedDown":true,"consensus":[]},"dy":{"Item1":null,"Item2":null},"p_L":{"Item1":null,"Item2":null},"peg_Ratio":{"Item1":null,"Item2":null},"p_VP":{"Item1":null,"Item2":null},"p_Ativo":{"Item1":null,"Item2":null},"margemBruta":{"Item1":null,"Item2":null},"margemEbit":{"Item1":null,"Item2":null},"margemLiquida":{"Item1":null,"Item2":null},"p_Ebit":{"Item1":null,"Item2":null},"eV_Ebit":{"Item1":null,"Item2":null},"dividaLiquidaEbit":{"Item1":null,"Item2":null},"dividaliquidaPatrimonioLiquido":{"Item1":null,"Item2":null},"p_SR":{"Item1":null,"Item2":null},"p_CapitalGiro":{"Item1":null,"Item2":null},"p_AtivoCirculante":{"Item1":null,"Item2":null},"roe":{"Item1":null,"Item2":null},"roic":{"Item1":null,"Item2":null},"roa":{"Item1":null,"Item2":null},"liquidezCorrente":{"Item1":null,"Item2":null},"pl_Ativo":{"Item1":null,"Item2":null},"passivo_Ativo":{"Item1":null,"Item2":null},"giroAtivos":{"Item1":null,"Item2":null},"receitas_Cagr5":{"Item1":null,"Item2":null},"lucros_Cagr5":{"Item1":null,"Item2":null},"liquidezMediaDiaria":{"Item1":null,"Item2":null},"vpa":{"Item1":null,"Item2":null},"lpa":{"Item1":null,"Item2":null},"valorMercado":{"Item1":null,"Item2":null}}&CategoryType=1'

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

dat = {'q':'goog'}
response = requests.get(url, headers=headers, params=dat)
dados = json.loads(response.text)

df = pd.DataFrame(dados)
df.columns = df.columns.str.lower()

print('Web Scraper executado com sucesso!')
````
Ainda no mesmo script Python, após a extração e conversão dos dados em formato Json para um pandas DataFrame, o programa Python realiza a conexão com o banco de dados Posrgres, com um cursor é realizada a criação da tabela de armazenamento de dados com a tipagem correta caso ela ainda não existir, também é realizado a truncagem da tabela para fazer a deleção de possíveis dados antigos, em seguida é realizado a importação dos dados recentemente exportados pelo Web Scraper.

```
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
```
Em seguida, o programa acessa um script SQL e faz a leitura de seu código, após a leitura o cursor executa o código no banco de dados, o código SQL está configurado para salvar o resultado de uma Query em uma tabela fixa, essa etapa é responsável pela criação dos dados dos rankings dos investidores fundamentalistas Benjamin Graham, Joel Greenblatt e a combinação de ambos.

```
with open(r'C:\Users\Mike\Desktop\Projeto acoes\sql\rankings.sql', 'r') as arq:
	sql = arq.read()

cur.execute(sql)
connection_data.commit()
print('Tabela de rankings atualizada!')
```

Código SQL:
```
drop table if exists investimentos.rankings_fundamentalistas;
select * into investimentos.rankings_fundamentalistas from (
with 
inicial_rank as (
	SELECT 
		ticker, 
		p_l, 
		row_number () over(ORDER BY p_l) as RANK_P_L,
		p_vp, 
		row_number () over(ORDER BY p_vp) as RANK_P_VP,
		roe,
		row_number () over(ORDER BY roe desc) as RANK_ROE
	FROM investimentos.acoes_cotacao as ac
	where ac.liquidezmediadiaria > 1000000
	and ac.p_l > 0
	and ac.p_vp > 0
	),
rank_bruto as (
	select 
		ir.*,
		ir.RANK_P_L  + ir.RANK_P_VP as RANK_Grahan_bruto,
		ir.RANK_P_L  + ir.RANK_ROE as RANK_Joel_bruto
	from inicial_rank as ir
	),
rank_bruto2 as (
	select rb.*,  rb.RANK_Grahan_bruto + rb.RANK_Joel_bruto as RANK_Final_bruto
	from rank_bruto as rb
)
select ticker, 
		p_l, 
		row_number () over(ORDER BY p_l) as RANK_P_L,
		p_vp, 
		row_number () over(ORDER BY p_vp) as RANK_P_VP,
		roe,
		row_number () over(ORDER BY roe desc) as RANK_ROE,
		row_number () over(ORDER BY RANK_Grahan_bruto) as RANK_Grahann,
		row_number () over(ORDER BY RANK_Joel_bruto) as RANK_Joel,
		row_number () over(ORDER BY RANK_Final_bruto) as RANK_Final
from rank_bruto2
order by RANK_Final
) r;
```
Após essa etapa o script Python se finaliza com sucesso e como resultado já temos todos os dados necessários para se trabalhar no Power B.i. O projeto do Power B.I consultara os dados diretamente do banco de dados Postgres, algumas funcionalidades utiliza medidas <b>DAX</b> para a realização de algumas operações com os dados, os principais códigos DAX são para se calcular o valor Intrínseco  e a Margem de Segurança de cada ação.

```
Valor Intrínseco = 

var LPA = sum('investimentos acoes_cotacao'[lpa])
var VPA = sum('investimentos acoes_cotacao'[vpa])
var final = 22.5*LPA*VPA
return
SQRT(final)
```

```
Margem de Segurança = (DIVIDE([Valor Intrínseco],sum('investimentos acoes_cotacao'[price]))-1)*100
```

Vale ressaltar que para a criação de toda a interface das dashboards foi utilizado a ferramenta figma.
<p align="center">
  <img  src="prints/2_figma.png">
  <b>Interfaces Figma</b>
</p>

## Resultados
<b>Problemas resolvidos:</b> A grande dificuldade do projeto ficou em torno do conhecimento das lógicas aplicados pelos conceitos de Benjamin Graham e Joel Greenblatt, tive que me aprofundar mais no conhecimento de indicadores fundamentalista de ações e no conceito que cada investidor para se fazer   os rankings de pré-seleção de ações. Como fonte de pesquisa foi utilizado o conteúdo disponibilizado pelo investidor [Vitor Senna](https://www.youtube.com/c/VitorSennaInvestimentos) e sites como [Suno](https://www.youtube.com/c/VitorSennaInvestimentos) e [Clube do Valor](https://www.youtube.com/c/VitorSennaInvestimentos)

<b>Resultado teórico:</b> Após estudos, foi possível constatar que Benjamin Graham prioriza as ações que possuem um menor preço sobre lucro, junto de um menor preço sobre valor patrimonial, ou seja, preferencia para ações descontadas. Já Joel Greenblatt prioriza ações com um menor preço sobre lucro, junto de uma maior eficiência, assim priorizando um desconto no preço da ação com a capacidade de eficiência da empresa. No dashboard de rankings o Preço sobre lucro é medido através do indicador P/L, Preço sobre valor patrimonial é medido pelo indicador P/VP e a eficiência é medida pelo ROE.

<b>Resultados finais:</b>  

## Capa
Capa do projeto, aqui se encontra os botões de acesso para as dashboards atualmente disponíveis.

<p align="center">
  <img  src="prints/3_capa.png">
</p>

## Rankings Fundamentalistas
Aqui é possível analisar os 3 diferentes rankings, Grahan, Joel e o ranking que compõem a combinação de metodologia dos dois investidores juntos, ainda nos próprios rankings e na barra de pequisa é possível selecionar uma ou um grupo de ações especificas para análise, por último temos uma lista de ações completa que contem todos os indicadores de uma determinada ação. Ao passar o mouse em cima dos ícones de informação "!", é possível visualizar mais informações. Vale ressaltar que para essa análise são excluídas as empresas com a Liquidez media diária menor que 1 milhão, assim retiramos as empresas com poucas negociações, também é retirado ações que contem com preço sobre lucro(P/L) negativo ou preço sobre valor patrimonial(P/VP) negativo, assim evitando empresas que estão dando prejuízo ou estão endividadas.

<p align="center">
  <img  src="prints/4_dash_rankings.png">
</p>

## Valor Intrínseco e margem de seguraça
Na dashboard nomeada com o nome de margem se segurança, é possível selecionar uma ação e através da fórmula original de Benjamin Graham: VI= √ (22,5 x LPA x VPA) é calculado automaticamente o valor intrínseco e a margem de segurança da ação selecionada.

<p align="center">
  <img  src="prints/5_dash_margem.png">
</p>

