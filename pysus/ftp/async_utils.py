from aioftp import Client

from . import File, line_file_parser
from pysus.online_data import CACHEPATH


async def download(file: File, local_dir: str = CACHEPATH):
    output = (
        local_dir+file.basename 
        if local_dir.endswith("/") 
        else local_dir+"/"+file.basename
    )
    async with Client.context(
        host="ftp.datasus.gov.br", 
        parse_list_line_custom=line_file_parser
    ) as client:
        await client.login()
        await client.download(file.path, output, write_into=True)   
