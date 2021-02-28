'''
Helper functions to download official statistics from IBGE SIDRA
'''
import pandas as pd
from urllib.error import HTTPError
import requests
import json

APIBASE = "https://servicodados.ibge.gov.br/api/v3/"

def get_sidra_table(table_id, territorial_level, geocode='all',
                    period=None, variables=None, classification=None, categories=None,
                    format=None, decimals=None, headers=None):
    """
    Wrapper for the SIDRA API. More information here: http://api.sidra.ibge.gov.br/home/ajuda
    :param table_id: código da tabela de onde se deseja extrair os dados. código pode ser obtido aqui: https://sidra.ibge.gov.br/acervo#/S/Q
    :param territorial_level: 1 – Brasil, 2 – Grande Região, 3 – Unidade da Federação, 6 – Município, etc
    :param geocode: geocódigo do IBGE: 3304557,3550308 – especifica os municípios do Rio de Janeiro e São Paulo.
    all – especifica todos os municípios. in n3 11,12 - especifica os municípios contidos nas Unidades da Federação Rondônia e Acre.

    :param period:  Os períodos podem ser especificados de forma avulsa, separados por vírgula (,), em faixas, separados por traço (-), ou de ambas as formas
        Um período pode ter o formato AAAA, de 4 dígitos, que representa um ano, ou o formato AAAADD, de 6 dígitos, onde AAAA representa um ano e DD seu correspondente mês (01 a 12), trimestre (01 a 04), semestre (01 a 02), etc, de acordo com a periodicidade de divulgação dos dados da tabela.

        Exemplo 1: /p/2008,2010-2012 – especifica os anos de 2008, e 2010 a 2012.

        Exemplo 2: /p/201101-201112,201204,201208 – especifica os meses de janeiro a dezembro de 2012, abril de 2012 e agosto de 2012.

        O parâmetro p pode ser seguido pela constante all para especificar todos os períodos disponíveis.

        Exemplo 3: /p/all

        O parâmetro p pode ser seguido pela constante first e um número de períodos, indicando os primeiros períodos da lista de períodos disponíveis (períodos mais antigos).
        O número de períodos pode ser omitido quando se tratar de apenas um aperíodo.

        Exemplo 4: /p/first 12

        Exemplo 5: /p/first

        O parâmetro p pode ser seguido pela constante last e um número de períodos, indicando os últimos períodos da série (períodos mais recentes).
        O número de períodos pode ser omitido quando se tratar de apenas um período.

        Exemplo 6: /p/last 12

        Exemplo 7: /p/last (valor default, quando não especificado o parâmetro p)
    :param variables:As variáveis são especificadas através de seus códigos, separados por vírgula (,).
        A lista de variáveis pode incluir também as variáveis de percentual geradas automaticamente pelo Sidra (são variáveis cujos códigos são superiores a 1.000.000).

        Exemplo 1: /v/63,69 – especifica o percentual no mês e o percentual acumulado no ano do IPCA.

        O parâmetro v pode ser seguido pela constante all para especificar todas as variáveis da tabela, inclusive as variáveis de percentual geradas automaticamente pelo Sidra.

        Exemplo 2: /v/all

        O parâmetro v pode ser seguido pela constante allxp para especificar todas as variáveis da tabela, exceto as variáveis de percentual geradas automaticamente pelo Sidra.

        Exemplo 3: /v/allxp (valor default, quando não especificado o parâmetro v)
    :param classification: informa o código de uma das classificações da tabela.
        Como exemplos, temos 1 – Situação do domicílio, 2 – Sexo, 81 – Produto da lavoura temporária, etc.
    :param categories:  As categorias são especificadas através de seus códigos, de forma individual ou para compor uma soma, separadas por vírgula (,).
        As categorias que compõem a soma devem ser separadas por espaço.

        Exemplo 1: /c81/2692,2702,2694 2695 – especifica os produtos da lavoura temporária arroz, feijão e (batata doce + batata inglesa)
    :param format:
    :param decimals:
    :param headers: `y` para receber o header (valor default, caso o parâmetro h não seja especificado). `n` para não receber o header.
    :return:
    """
    base_url = "https://apisidra.ibge.gov.br/values"
    query = f"/t/{table_id}/n{territorial_level}/{geocode}"
    if period is not None:
        query += f"/p/{period}"
    if variables is not None:
        query += f"/v/{variables}"
    if classification is not None:
        query += f"/c{classification}"
    if categories is not None:
        query += f"/{categories}"
    if format is not None:
        query += f"/f/{format}"
    if decimals is not None:
        query += f"/d/{decimals}"
    if headers is not None:
        query += f"/h/{headers}"

    url = base_url + query
    print(f'Requesting data from {url}')
    try:
        df = pd.read_json(url)
    except HTTPError as exc:
        response =requests.get(url)
        print(f"Consulta falhou: {response.text}")
        return None
    return df


def list_agregados(**kwargs):
    """
    Lista de agregados agrupados por pesquisa.
    veja https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-Agregados-agregadosGet
    para maiores detalhes
    :param kwargs: parâmetros válidos: período, assunto, classificacao, periodicidade,nivel.
    :return: Dataframe
    """
    url = APIBASE + "agregados?"
    url += "&".join([f"{k}={v}" for k, v in kwargs.items()])
    try:
        table = pd.read_json(url)
    except:
        return None
    return table

def localidades_por_agregado(agregado: int, nivel: str):
    """
    Obtém as localidades associadas ao agregado de acordo com um ou mais níveis geográficos.
    :param agregado: codigo numérico do agregado
    :param nivel: Identificador do nível geográfico ao qual pertence as localidades. Pode conter um ou mais níveis
    delimitados pelo caracter | (pipe). p.ex. N7|N6
    :return:
    """
    url = APIBASE + f"agregados/{agregado}/localidades/{nivel}"
    try:
        table = pd.read_json(url)
    except Exception as e:
        print(f"Could not download from {url}\n{e}")
        return None
    return table

def metadados(agregado: int):
    """
    Obtém os metadados associados ao agregado

    :param agregado: Identificador do agregado
    """
    url = APIBASE + f"agregados/{agregado}/metadados"
    try:
        res = requests.get(url)
        data = res.json()
    except Exception as e:
        print(f"Could not download from {url}\n{e}")
        return None
    return data

def lista_periodos(agregado: int):
    """
    Obtém os períodos associados ao agregado
    :param agregado:
    :return:
    """
    url = APIBASE + f"agregados/{agregado}/periodos"
    try:
        table = pd.read_json(url)
    except:
        return None
    return table


class FetchData:
    """
       Obtém o conjunto de variáveis a partir do identificador do agregado, períodos pesquisados e identificador das variáveis
       :param agregado: identifocador do agregados
       :param periodos: Período do qual se deseja obter os resultados. Consulte os identificadores dos períodos na Base de
           identificadores. Informe valores negativos para obter os últimos resultados. Pode conter um ou mais períodos
           delimitados pelo caracter | (pipe)
       :param variavel: Um ou mais identificadores de variável separados pelo caracter | (pipe). Caso omitido, assume o
           valor allxp, que retorna quaisquer variáveis relacionada ao agregado. Para saber mais sobre as variáveis de cada
           agregado, acesse seus respectivos metadados
       :kwargs: parametros adicionais:
           - **localidades**: Uma ou mais localidades delimitadas pelo caracter | (pipe). No caso do Brasil, o identificador é
               BR. Para qualquer outra localidade que NÃO seja Brasil, essa deve seguir o padrão N<NIVEL_GEOGRAFICO>[<LOCALIDADE>],
               em que <LOCALIDADE> pode ser uma ou mais localidades separadas por vírgula. É possível ainda generalizar o
               resultado, informando a classe da localidade, conforme os exemplos a seguir

                   https://servicodados.ibge.gov.br/api/v3/agregados/1705/variaveis?localidades=N7

               Obtém os resultados referentes às variáveis do agregado 1705 cujas localidades sejam regiões metropolitanas (N7)

                   https://servicodados.ibge.gov.br/api/v3/agregados/1705/variaveis?localidades=N7[3501,3301]

               Obtém os resultados referentes às variáveis do agregado 1705 cujas localidades sejam as regiões metropolitanas
               (N7) de São Paulo e Rio de Janeiro (3501,3301). Observe que 3501 e 3301 são, respectivamente, os identificadores
               das regiões metropolitanas de São Paulo e Rio de Janeiro. Não podem ser confundidos, portanto, com os
               identificadores dos municípios de São Paulo/SP e Rio de Janeiro/RJ, que são 3550308 e 3304557, respectivamente

           - **classificacao**: Além de estar relacionado à uma dada localidade e um determinado período, os resultados das
               variáveis podem estar relacionados à outros conjuntos de dados, que na nomenclatura do SIDRA recebe o nome de
               classificação. Como exemplo, considere o agregado Produção, venda, valor da produção e área colhida da lavoura
               temporária nos estabelecimentos agropecuários. Além da localidade e do período, os resultados produzidos por
               esse agregado referem-se aos produtos produzidos, condição do produtor, grupos de atividades econômica, grupos
               de área, grupos de área colhida e pronafiano, que são as classificações do agregado - Para conhecer as
               classificações de cada agregado, acesse seus respectivos metadados. Aos componentes da classificação, dar-se o
               nome de categoria. Na prática, você fará uso das classificações para restringir a consulta, conforme os exemplos a seguir

               https://servicodados.ibge.gov.br/api/v3/agregados/1712/variaveis?classificacao=226[4844]&localidades=BR

               Obtém os resultados referentes às variáveis do agregado 1712 cujo produto produzido (226) seja abacaxi (4844)
               no Brasil (BR)

               https://servicodados.ibge.gov.br/api/v3/agregados/1712/variaveis?classificacao=226[4844]|218[4780]&localidades=BR

               Obtém os resultados referentes às variáveis do agregado 1712 cujo produto produzido (226) seja abacaxi (4844) e
               cuja condição do produtor (218) seja proprietário (4780) no Brasil (BR)
           - **view**: Modo de visualização. Caso deseje que a resposta seja renderizada usando notação OLAP, configure
               esse parâmetro com o valor OLAP - https://servicodados.ibge.gov.br/api/v3/agregados/1705/variaveis?view=OLAP&localidades=BR.
               A outra opção é configurar esse parâmetro com o valor flat. No modo flat, o primeiro elemento do Array são
               metadados, de forma que os resultados vêm a partir do segundo elemento
       """
    def __init__(self, agregado: int, periodos: str, variavel: str='allxp', **kwargs):
        self.url = APIBASE + f"agregados/{agregado}/periodos/{periodos}/variaveis/{variavel}?"
        self.url += "&".join([f"{k}={v}" for k, v in kwargs.items()])
        self.JSON = None
        self._fetch_JSON()

    def _fetch_JSON(self):
        try:
            print(f"Fetching {self.url}" )
            res = requests.get(self.url)
            self.JSON = res.json()
        except Exception as e:
            print(f"Couldn't download data:\n{e}")

    def to_dataframe(self):
        return pd.DataFrame(self.JSON)




