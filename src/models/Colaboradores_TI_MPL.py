
import requests
import pandas as pd




class Colaboradores():
    '''Classe responsavel por buscar na API interna do TI dados relacionados aos colaboradores'''

    def __init__(self, codcolaborador = ''):

        self.codcolaborador = codcolaborador

    def get_colaborador(self):
        '''Metodo que busca os colaborador'''

        api_url = 'http://10.162.0.202:3001/api/colaboradores'

        try:
            # 1. Faz a requisição GET para a API
            print(f"Buscando dados em: {api_url}...")
            response = requests.get(api_url)

            # 2. Verifica se a requisição foi bem-sucedida (status code 200)
            response.raise_for_status()

            # 3. Converte a resposta JSON em uma lista de dicionários
            data = response.json()

            # 4. Cria o DataFrame do Pandas
            df = pd.DataFrame(data)

            df['id'] = df['id'].astype(str)

            print("DataFrame criado com sucesso!")
            return df

        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados da API: {e}")
            return None
        except ValueError:
            print("Erro: A resposta da API não está no formato JSON esperado.")
            return None