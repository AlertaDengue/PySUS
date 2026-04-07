from typing import Any, Dict, List, Literal, Optional, Union

from pysus.api.ftp.models import Dataset, Directory, File
from pysus.utils import MONTHS, UFs, parse_UFs, to_list, zfill_year


class CIHA(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/CIHA/201101_/Dados"),
    ]

    group_definitions: Dict[str, str] = {
        "CIHA": "Comunicação de Internação Hospitalar e Ambulatorial",
    }

    @property
    def name(self) -> str:
        return "CIHA"

    @property
    def long_name(self) -> str:
        return "Comunicação de Internação Hospitalar e Ambulatorial"

    @property
    def description(self) -> str:
        return (
            "A CIHA foi criada para ampliar o processo de planejamento, "
            "programação, controle, avaliação e regulação da assistência à "
            "saúde permitindo um conhecimento mais abrangente e profundo dos "
            "perfis nosológico e epidemiológico da população brasileira."
        )

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            group_code = name[:4]
            state = name[4:6]
            year_short = name[6:8]
            month = name[8:10]

            group_info = None
            if group_code in self.group_definitions:
                group_info = {
                    "name": group_code,
                    "long_name": self.group_definitions[group_code],
                    "description": None,
                }

            return {
                "group": group_info,
                "state": state,
                "year": (
                    int(f"20{year_short}")
                    if int(year_short) < 80
                    else int(f"19{year_short}")
                ),
                "month": int(month),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None, "month": None}

    async def get_files(
        self,
        state: Optional[Union[List[str], str]] = None,
        year: Optional[Union[List[int], int]] = None,
        month: Optional[Union[List[int], int]] = None,
        group: Optional[Union[List[str], str]] = "CIHA",
    ) -> List[File]:
        return await self.search(
            state=state,
            year=year,
            month=month,
            group=group,
        )


class CNES(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/CNES/200508_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
        "DC": "Dados Complementares",
        "EE": "Estabelecimento de Ensino",
        "EF": "Estabelecimento Filantrópico",
        "EP": "Equipes",
        "EQ": "Equipamentos",
        "GM": "Gestão e Metas",
        "HB": "Habilitação",
        "IN": "Incentivos",
        "LT": "Leitos",
        "PF": "Profissional",
        "RC": "Regra Contratual",
        "SR": "Serviço Especializado",
        "ST": "Estabelecimentos",
    }
    __loaded__ = set()

    @property
    def name(self) -> str:
        return "CNES"

    @property
    def long_name(self) -> str:
        return "Cadastro Nacional de Estabelecimentos de Saúde"

    @property
    def description(self) -> str:
        return """
            O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o
            sistema de informação oficial de cadastramento de informações
            de todos os estabelecimentos de saúde no país, independentemente
            de sua natureza jurídica ou de integrarem o Sistema Único de
            Saúde (SUS). Trata-se do cadastro oficial do Ministério da
            Saúde (MS) no tocante à realidade da capacidade instalada e
            mão-de-obra assistencial de saúde no Brasil em estabelecimentos
            de saúde públicos ou privados, com convênio SUS ou não.
        """

    def load(
        self,
        groups: Union[str, List[str]] = None,
    ):
        """
        Loads CNES Groups into content. Will convert the files and directories
        found within FTP Directories into self.content
        """
        if not self.__content__:
            self.paths[0].load()
            self.__content__ |= self.paths[0].__content__

        if groups:
            groups = to_list(groups)

            if not all(group in self.groups for group in [gr.upper() for gr in groups]):
                raise ValueError(
                    f"Unknown CNES group(s): {set(
                        groups).difference(self.groups)}"
                )

            for group in groups:
                group = group.upper()
                if group not in self.__loaded__:
                    directory = self.__content__[group]
                    directory.load()
                    self.__content__ |= directory.__content__
                    self.__loaded__.add(directory.name)
        return self

    def describe(self, file: File):
        if not isinstance(file, File) or file.name == "GMufAAmm":
            return None

        if file.extension.upper() not in [".DBC", ".DBF"]:
            return None

        group, _uf, year, month = self.format(file)

        return FileDescription(
            name=str(file.basename),
            group=self.groups.get(group, group),
            uf=UFs.get(_uf, _uf),
            month=MONTHS.get(int(month), month),
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        group, _uf = file.name[:2].upper(), file.name[2:4].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, _uf, zfill_year(year), month

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        if not group:
            raise ValueError("At least one CNES group is required")

        groups = [gr.upper() for gr in to_list(group)]

        self.load(groups)

        files = list(filter(lambda f: f.name[:2] in groups, self.files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: f.name[2:4] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [str(m)[-2:].zfill(2) for m in to_list(year)]
            files = list(filter(lambda f: f.name[-4:-2] in years, files))

        if month:
            months = [str(y)[-2:].zfill(2) for y in to_list(month)]
            files = list(filter(lambda f: f.name[-2:] in months, files))

        return files


class IBGEDATASUS(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/IBGE/POP"),
        Directory("/dissemin/publicos/IBGE/censo"),
        Directory("/dissemin/publicos/IBGE/POPTCU"),
        Directory("/dissemin/publicos/IBGE/projpop"),
        # Directory("/dissemin/publicos/IBGE/Auxiliar") # this has a different file name pattern  # noqa
    ]

    @property
    def name(self) -> str:
        return "IBGE"

    @property
    def long_name(self) -> str:
        return (
            "Populaçao Residente, Censos, Contagens "
            "Populacionais e Projeçoes Intercensitarias"
        )

    @property
    def description(self) -> str:
        return """
            São aqui apresentados informações sobre a população residente,
            estratificadas por município, faixas etárias e sexo, obtidas a
            partir dos Censos Demográficos, Contagens Populacionais
            e Projeções Intercensitárias.
        """

    def describe(self, file: File):
        ext = file.extension.upper()

        if ext == ".ZIP":
            year = file.name.split(".")[0][-2:]
        elif ext == ".DBF":
            year = file.name[-2:]
        else:
            return None

        return FileDescription(
            name=str(file.basename),
            group="Population",
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        return (file.name[-2:],)

    def get_files(
        self,
        source: Literal["POP", "censo", "POPTCU", "projpop"] = "POPTCU",
        year: Optional[Union[str, int, list]] = None,
        *args,
        **kwargs,
    ) -> List[File]:
        sources = ["POP", "censo", "POPTCU", "projpop"]
        source_dir = None

        for dir in self.paths:
            if source in sources and source in dir.path:
                source_dir = dir

        if not source_dir:
            raise ValueError(f"Unkown source {source}. Options: {sources}")

        files = source_dir.content

        if year:
            if isinstance(year, (str, int)):
                files = [
                    f for f in files if self.describe(f)["year"] == zfill_year(year)
                ]
            elif isinstance(year, list):
                files = [
                    f
                    for f in files
                    if str(self.describe(f)["year"])
                    in [str(zfill_year(y)) for y in year]
                ]

        return files


class PNI(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/PNI/DADOS"),
    ]
    group_definitions: Dict[str, str] = {
        "CPNI": "Cobertura Vacinal",
        "DPNI": "Doses Aplicadas",
    }

    @property
    def name(self) -> str:
        return "PNI"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações do Programa Nacional de Imunizações"

    @property
    def description(self) -> str:
        return """
            O SI-PNI é um sistema desenvolvido para possibilitar aos
            gestores envolvidos no Programa Nacional de Imunização, a
            avaliação dinâmica do risco quanto à ocorrência de surtos ou
            epidemias, a partir do registro dos imunobiológicos aplicados e
            do quantitativo populacional vacinado, agregados por faixa
            etária, período de tempo e área geográfica. Possibilita também
            o controle do estoque de imunobiológicos necessário aos
            administradores que têm a incumbência de programar sua aquisição
            e distribuição. Controla as indicações de aplicação de
            vacinas de imunobiológicos especiais e seus eventos adversos,
            dentro dos Centros de Referências em imunobiológicos especiais.
        """

    def describe(self, file: File):
        if not isinstance(file, File) or file.extension.upper() not in [
            ".DBC",
            ".DBF",
        ]:
            return None

        group, _uf, year = self.format(file)

        return FileDescription(
            name=str(file.basename),
            group=self.groups.get(group, group),
            uf=UFs.get(_uf, _uf),
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        if len(file.name) != 8:
            raise ValueError(f"Can't format {file.name}")

        n = file.name
        group, _uf, year = n[:4], n[4:6], n[-2:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[list, Literal["CNPI", "DPNI"]],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = list(
            filter(lambda f: f.extension.upper()
                   in [".DBC", ".DBF"], self.files)
        )

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                f"Unknown PNI Group(s): {set(
                    groups).difference(list(self.groups))}"
            )

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(m)[-2:]) for m in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files


class SIA(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIASUS/199407_200712/Dados"),
        Directory("/dissemin/publicos/SIASUS/200801_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
        "AB": "APAC de Cirurgia Bariátrica",
        "ABO": "APAC de Acompanhamento Pós Cirurgia Bariátrica",
        "ACF": "APAC de Confecção de Fístula",
        "AD": "APAC de Laudos Diversos",
        "AM": "APAC de Medicamentos",
        "AMP": "APAC de Acompanhamento Multiprofissional",
        "AN": "APAC de Nefrologia",
        "AQ": "APAC de Quimioterapia",
        "AR": "APAC de Radioterapia",
        "ATD": "APAC de Tratamento Dialítico",
        "BI": "Boletim de Produção Ambulatorial individualizado",
        "IMPBO": "",  # TODO
        "PA": "Produção Ambulatorial",
        "PAM": "",  # TODO
        "PAR": "",  # TODO
        "PAS": "",  # TODO
        "PS": "RAAS Psicossocial",
        "SAD": "RAAS de Atenção Domiciliar",
    }

    @property
    def name(self) -> str:
        return "SIA"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações Ambulatoriais"

    @property
    def description(self) -> str:
        return """
            O Sistema de Informação Ambulatorial (SIA) foi instituído pela
            Portaria GM/MS n.º 896 de 29 de junho de 1990. Originalmente, o
            SIA foi concebido a partir do projeto SICAPS (Sistema de
            Informação e Controle Ambulatorial da Previdência Social), em
            que os conceitos, os objetivos e as diretrizes criados para o
            desenvolvimento do SICAPS foram extremamente importantes e
            amplamente utilizados para o desenvolvimento do SIA, tais
             como: (i) o acompanhamento das programações físicas e
            orçamentárias; (ii) o acompanhamento das ações de saúde
            produzidas; (iii) a agilização do pagamento e controle
            orçamentário e financeiro; e (iv) a formação de banco de dados
            para contribuir com a construção do SUS.
        """

    def describe(self, file: File):
        if file.extension.upper() != ".DBC":
            return None

        group_code, _uf, year, month = self.format(file)

        return FileDescription(
            name=str(file.basename),
            group=self.groups.get(group_code, group_code),
            uf=UFs.get(_uf, _uf),
            month=MONTHS.get(int(month), str(month)),
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        if file.extension.upper() in [".DBC", ".DBF"]:
            digits = "".join([d for d in file.name if d.isdigit()])
            if "_" in file.name:
                name, _ = file.name.split("_")
                digits = "".join([d for d in name if d.isdigit()])
            chars, _ = file.name.split(digits)
            year, month = digits[:2], digits[2:]
            group, uf = chars[:-2].upper(), chars[-2:].upper()
            return group, uf, zfill_year(year), month
        return ()

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = list(
            filter(lambda f: f.extension.upper()
                   in [".DBC", ".DBF"], self.files)
        )

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                f"Unknown SIA Group(s): {set(
                    groups).difference(list(self.groups))}"
            )

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(m)[-2:]) for m in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        if month:
            months = [str(y)[-2:].zfill(2) for y in to_list(month)]
            files = list(filter(lambda f: self.format(f)[3] in months, files))

        return files


class SIH(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIHSUS/199201_200712/Dados"),
        Directory("/dissemin/publicos/SIHSUS/200801_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
        "RD": "AIH Reduzida",
        "RJ": "AIH Rejeitada",
        "ER": "AIH Rejeitada com erro",
        "SP": "Serviços Profissionais",
        "CH": "Cadastro Hospitalar",
        "CM": "",  # TODO
    }

    @property
    def name(self) -> str:
        return "SIH"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações Hospitalares"

    @property
    def description(self) -> str:
        return """
            A finalidade do AIH (Sistema SIHSUS) é a de transcrever todos os
            atendimentos que provenientes de internações hospitalares que
            foram financiadas pelo SUS, e após o processamento, gerarem
            relatórios para os gestores que lhes possibilitem fazer os
            pagamentos dos estabelecimentos de saúde. Além disso, o nível
            Federal recebe mensalmente uma base de dados de todas as
            internações autorizadas (aprovadas ou não para pagamento) para
            que possam ser repassados às Secretarias de Saúde os valores de
            Produção de Média e Alta complexidade além dos valores de CNRAC,
            FAEC e de Hospitais Universitários – em suas variadas formas de
            contrato de gestão.
        """

    def describe(self, file: File):
        if not isinstance(file, File) or file.extension.upper() not in [
            ".DBC",
            ".DBF",
        ]:
            return None

        group_code, _uf, year, month = self.format(file)

        return FileDescription(
            name=str(file.basename),
            group=self.groups.get(group_code, group_code),
            uf=UFs.get(_uf, _uf),
            month=MONTHS.get(int(month), str(month)),
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        group, _uf = file.name[:2].upper(), file.name[2:4].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, _uf, zfill_year(year), month

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = list(
            filter(lambda f: f.extension.upper()
                   in [".DBC", ".DBF"], self.files)
        )

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                f"Unknown SIH Group(s): {set(
                    groups).difference(list(self.groups))}"
            )

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(m)[-2:]) for m in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        if month:
            months = [str(y)[-2:].zfill(2) for y in to_list(month)]
            files = list(filter(lambda f: self.format(f)[3] in months, files))

        return files


class SIM(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIM/CID10/DORES"),
        Directory("/dissemin/publicos/SIM/CID9/DORES"),
    ]
    group_definitions: Dict[str, str] = {"CID10": "DO", "CID9": "DOR"}

    @property
    def name(self) -> str:
        return "SIM"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação sobre Mortalidade"

    @property
    def description(self) -> str:
        return ""

    def describe(self, file: File):
        group, _uf, year = self.format(file)
        groups = {v: k for k, v in self.groups.items()}

        return FileDescription(
            name=str(file.basename),
            uf=UFs.get(_uf, _uf),
            year=year,
            group=groups.get(group, group),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        if "CID9" in str(file.path):
            group, _uf, year = file.name[:-4], file.name[-4:-2], file.name[-2:]
        else:
            group, _uf, year = file.name[:-6], file.name[-6:-4], file.name[-4:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[list[str], str],
        uf: Optional[Union[list[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = self.files

        groups = [self.groups[g.upper()] for g in to_list(group)]

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(y) for y in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files


class SINAN(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SINAN/DADOS/FINAIS"),
        Directory("/dissemin/publicos/SINAN/DADOS/PRELIM"),
    ]

    group_definitions: Dict[str, str] = {
        "ACBI": "Acidente de trabalho com material biológico",
        "ACGR": "Acidente de trabalho",
        "ANIM": "Acidente por Animais Peçonhentos",
        "ANTR": "Atendimento Antirrabico",
        "BOTU": "Botulismo",
        "CANC": "Cancêr relacionado ao trabalho",
        "CHAG": "Doença de Chagas Aguda",
        "CHIK": "Febre de Chikungunya",
        "COLE": "Cólera",
        "COQU": "Coqueluche",
        "DENG": "Dengue",
        "DERM": "Dermatoses ocupacionais",
        "DIFT": "Difteria",
        "ESQU": "Esquistossomose",
        "EXAN": "Doença exantemáticas",
        "FMAC": "Febre Maculosa",
        "FTIF": "Febre Tifóide",
        "HANS": "Hanseníase",
        "HANT": "Hantavirose",
        "HEPA": "Hepatites Virais",
        "IEXO": "Intoxicação Exógena",
        "INFL": "Influenza Pandêmica",
        "LEIV": "Leishmaniose Visceral",
        "LEPT": "Leptospirose",
        "LERD": "LER/Dort",
        "LTAN": "Leishmaniose Tegumentar Americana",
        "MALA": "Malária",
        "MENI": "Meningite",
        "MENT": "Transtornos mentais relacionados ao trabalho",
        "NTRA": "Notificação de Tracoma",
        "PAIR": "Perda auditiva por ruído relacionado ao trabalho",
        "PEST": "Peste",
        "PFAN": "Paralisia Flácida Aguda",
        "PNEU": "Pneumoconioses realacionadas ao trabalho",
        "RAIV": "Raiva",
        "SDTA": "Surto Doenças Transmitidas por Alimentos",
        "SIFA": "Sífilis Adquirida",
        "SIFC": "Sífilis Congênita",
        "SIFG": "Sífilis em Gestante",
        "SRC": "Síndrome da Rubéola Congênia",
        "TETA": "Tétano Acidental",
        "TETN": "Tétano Neonatal",
        "TOXC": "Toxoplasmose Congênita",
        "TOXG": "Toxoplasmose Gestacional",
        "TRAC": "Inquérito de Tracoma",
        "TUBE": "Tuberculose",
        "VARC": "Varicela",
        "VIOL": "Violência doméstica, sexual e/ou outras violências",
        "ZIKA": "Zika Vírus",
    }

    @property
    def name(self) -> str:
        return "SINAN"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação de Agravos de Notificação Compulsória"

    @property
    def description(self) -> str:
        return """
           O Sistema de Informação de Agravos de Notificação Compulsória
           (sINAN) é alimentado principalmente pela notificação e investigação
           de casos de doenças e condições listadas na lista nacional de agravos
           de notificação compulsória (Portaria Consolidada nº 4, de 28 de
           setembro de 2017, Anexo). No entanto, os estados e municípios podem
           incluir outros problemas de saúde importantes em sua região, como a
           difilobotriose no município de São Paulo. Seu uso efetivo permite o
           diagnóstico dinâmico da ocorrência de um evento na população,
           fornecendo evidências para as explicações causais das agravos de
           notificação compulsória e indicando os riscos aos quais as pessoas
           estão expostas. Isso contribui para a identificação da realidade
           epidemiológica de uma área geográfica específica. Seu uso
           sistemático e descentralizado contribui para a democratização
           da informação, permitindo que todos os profissionais de saúde acessem
           e a disponibilizem à comunidade. Portanto, é um sistema de informação
           de agravos de notificação compulsória.
        """

    def describe(self, file: File):
        if not isinstance(file, File) or file.extension.upper() != ".DBC":
            return None

        dis_code, year = self.format(file)

        return FileDescription(
            name=str(file.basename),
            disease=self.diseases.get(dis_code, "Unknown"),
            group=dis_code,
            year=zfill_year(year),
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        year = file.name[-2:]

        if file.name.startswith("SRC"):
            dis_code = file.name[:3]
        elif file.name == "LEIBR22":
            dis_code = "LEIV"  # MISPELLED FILE NAME
        elif file.name == "LERBR19":
            dis_code = "LERD"  # ANOTHER ONE
        else:
            dis_code = file.name[:4]

        return dis_code, zfill_year(year)

    def get_files(
        self,
        dis_code: Optional[Union[str, list]] = None,
        year: Optional[Union[str, int, list]] = None,
    ) -> List[File]:
        files = list(
            filter(lambda f: f.extension.upper()
                   in [".DBC", ".DBF"], self.files)
        )

        if dis_code:
            codes = [c.upper() for c in to_list(dis_code)]

            if codes and not all(code in self.diseases for code in codes):
                raise ValueError(
                    f"Unknown disease(s): {set(
                        codes).difference(set(self.diseases))}"
                )

            files = list(filter(lambda f: self.format(f)[0] in codes, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(y)[-2:]) for y in to_list(year)]
            files = list(filter(lambda f: self.format(f)[1] in years, files))

        return files


class SINASC(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SINASC/NOV/DNRES"),
        Directory("/dissemin/publicos/SINASC/ANT/DNRES"),
    ]
    group_definitions: Dict[str, str] = {
        "DN": "Declarações de Nascidos Vivos",
        "DNR": "Dados dos Nascidos Vivos por UF de residência",
    }

    @property
    def name(self) -> str:
        return "SINASC"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações sobre Nascidos Vivos"

    @property
    def description(self) -> str:
        return """
            A finalidade do SINASC é fornecer subsídios para o diagnóstico de
            saúde, planejamento e gestão de políticas públicas voltadas à
            saúde da mulher e da criança. Através dele, é possível calcular
            indicadores vitais como a taxa de natalidade e monitorar fatores
            de risco para a mortalidade infantil, permitindo intervenções mais
            precisas nos níveis federal, estadual e municipal.
        """

    def describe(self, file: File):
        if not isinstance(file, File) or file.extension.upper() != ".DBC":
            return None

        group_code, _uf, year = self.format(file)

        return FileDescription(
            name=str(file.basename),
            group=self.groups.get(group_code, group_code),
            uf=UFs.get(_uf, _uf),
            year=year,
            size=file.info.get("size", 0),
            last_update=file.info.get("modify"),
        )

    def format(self, file: File) -> tuple:
        if file.name == "DNEX2021":
            pass

        year = zfill_year(file.name[-2:])
        charname = "".join([c for c in file.name if not c.isnumeric()])
        group, _uf = charname[:-2], charname[-2:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[List, str, int]] = None,
    ) -> List[File]:
        files = self.files

        groups = to_list(group)

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            if "EX" in to_list(uf):
                # DNEX2021
                if len(to_list(uf)) == 1:
                    return []

                to_list(uf).remove("EX")

            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(y)[-2:]) for y in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files


AVAILABLE_DATABASES = [
    CIHA,
    # CNES,
    # IBGEDATASUS,
    # PNI,
    # SIA,
    # SIH,
    # SIM,
    # SINAN,
    # SINASC,
]
