--RANK_Grahan: Prioriza um maior desconto, contem a combinação do menor preço sobre lucro (P/L) e o menor preço sobre valor patrimonial(P/VP).
--RANK_Joel: Prioriza um maior desconto e uma alta eficiência, a combinação do menor preço sobre lucro (P/L) e a alta eficiência (ROE).
--RANK_Final: A combinação do ranking de Grahan e de Joel, nesse ranking o menor preço sobre lucro (P/L) contem peso 2, a maior eficiência (ROE) contem peso 1 e o menor preço sobre valor patrimonial(P/VP) contem peso 1.
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