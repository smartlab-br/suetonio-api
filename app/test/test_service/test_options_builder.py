''' Testes do formatador '''
import unittest
from service.qry_options_builder import QueryOptionsBuilder

class OptionsBuilderTest(unittest.TestCase):
    ''' Classe que testa a construção de options a partir dos parâmetros do request '''
    def test_no_categories(self):
        ''' Verifica se o parâmetro obrigatório de categorias está presente '''
        self.assertRaises(ValueError, QueryOptionsBuilder.build_options, {})

    def test_full_args(self):
        ''' Verifica se os parâmetros são criados corretamente '''
        r_args = {
            "categorias": 'a,b',
            "valor": 'c,d',
            "agregacao": 'e,f',
            "ordenacao": 'g,h',
            "filtros": r'eq-o-comma\,separated,and,eq-p-q',
            "pivot": 'i,j',
            "limit": '10',
            "offset": '11',
            "calcs": 'k,l',
            "partition": 'm,n'
        }

        opts = QueryOptionsBuilder.build_options(r_args)

        self.assertEqual(
            opts,
            {
                "categorias": ['a', 'b'],
                "valor": ['c', 'd'],
                "agregacao": ['e', 'f'],
                "ordenacao": ['g', 'h'],
                "where": ['eq-o-comma,separated', 'and', 'eq-p-q'],
                "pivot": ['i', 'j'],
                "limit": '10',
                "offset": '11',
                "calcs": ['k', 'l'],
                "partition": ['m', 'n'],
            }
        )

class PersonOptionsBuilderTest(unittest.TestCase):
    ''' Classe que testa a construção de options a partir dos parâmetros do request '''
    def test_full_args_empresa(self):
        ''' Verifica se os parâmetros são criados corretamente para empresa '''
        r_args = {
            "dados": 'a',
            "competencia": 'b',
            "id_pf": '12345678900',
            "perspective": 'c',
            "only_meta": 'S',
            "reduzido": 'S',
            "id_inv": '00000000'
        }

        opts = QueryOptionsBuilder.build_person_options(r_args)

        self.assertEqual(
            opts,
            {
                "column_family": 'a',
                "column": 'b',
                "id_pf": '12345678900',
                "perspective": 'c',
                "only_meta": True,
                "reduzido": True,
                "cnpj_raiz": '00000000'
            }
        )

    def test_full_args_estabelecimento(self):
        ''' Verifica se os parâmetros são criados corretamente para empresa '''
        r_args = {
            "dados": 'a',
            "competencia": 'b',
            "id_pf": '12345678900',
            "perspective": 'c',
            "only_meta": 'S',
            "reduzido": 'S',
            "id_inv": '00000000000000'
        }

        opts = QueryOptionsBuilder.build_person_options(r_args, mod='estabelecimento')

        self.assertEqual(
            opts,
            {
                "column_family": 'a',
                "column": 'b',
                "id_pf": '12345678900',
                "perspective": 'c',
                "only_meta": True,
                "reduzido": True,
                "cnpj": '00000000000000',
                "cnpj_raiz": '00000000'
            }
        )
