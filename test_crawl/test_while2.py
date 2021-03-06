import sys
import io
import boto3
import pyarrow.parquet as pq

# The player's power starts out at 5.
power = 29
input_option = 'y'
l = ["fragrance_parquet/PRDSHPF.parquet/part-00000-cfabc819-7856-4933-aeb9-e9b15c088a67-c000.snappy.parquet",
     "fragrance_parquet/apapp100.parquet/part-00000-601d0b39-c238-4afb-a4f1-dfcf4f2f0eef-c000.snappy.parquet",
     "fragrance_parquet/apapp200.parquet/part-00000-fb7ac374-ce98-432f-b220-e400c69f57f6-c000.snappy.parquet",
     "fragrance_parquet/ararp100.parquet/part-00000-3827ef15-f786-41d4-9b4d-4e49f6504116-c000.snappy.parquet",
     "fragrance_parquet/ararp200.parquet/part-00000-91b8f3e9-9ea4-45c0-81ef-a29fe6cd3924-c000.snappy.parquet",
     "fragrance_parquet/arccp200.parquet/part-00000-90c9384f-f2fc-4895-97b1-39eddc716187-c000.snappy.parquet",
     "fragrance_parquet/glglp100.parquet/part-00000-9f928e6a-e7a3-46a5-85e0-29de4849e780-c000.snappy.parquet",
     "fragrance_parquet/glsrp100.parquet/part-00000-c30eca47-96b2-4c05-9961-0fa570987512-c000.snappy.parquet",
     "fragrance_parquet/inpop100.parquet/part-00000-7e69ffc8-3706-48f6-a333-077cb6c26b8d-c000.snappy.parquet",
     "fragrance_parquet/inpop10h.parquet/part-00000-bc3e4b60-ccd3-40e8-827b-864035514b4b-c000.snappy.parquet",
     "fragrance_parquet/inpop300.parquet/part-00000-55658409-1fc6-4583-82d6-f802bb132f13-c000.snappy.parquet",
     "fragrance_parquet/inpop30h.parquet/part-00000-5b2af8bd-352b-43dd-af2f-148723bc4ab8-c000.snappy.parquet",
     "fragrance_parquet/inpop400.parquet/part-00000-54b53bb0-9c0c-49b4-add0-240c2ea11374-c000.snappy.parquet",
     "fragrance_parquet/kcdep100.parquet/part-00000-2e1f4568-6d9a-4184-bea3-5a702f51a775-c000.snappy.parquet",
     "fragrance_parquet/kcfap100.parquet/part-00000-3282b42a-2c4e-449f-ad62-eb458bf08327-c000.snappy.parquet",
     "fragrance_parquet/kcfap200.parquet/part-00000-0640af5e-4dcc-4303-a52f-3b9bc22c6eb4-c000.snappy.parquet",
     "fragrance_parquet/kcfpp100.parquet/part-00000-299b48fa-b1f1-4348-ba0a-caf03ed83b47-c000.snappy.parquet",
     "fragrance_parquet/kcglp100.parquet/part-00000-a614b485-eb9c-46cd-b4fc-630d67998ced-c000.snappy.parquet",
     "fragrance_parquet/kcglp200.parquet/part-00000-41d9f1d4-bf98-4353-ae0d-9ffc9292e937-c000.snappy.parquet",
     "fragrance_parquet/kcgmp100.parquet/part-00000-74767361-4bdf-44d2-ba97-bb77658a6e14-c000.snappy.parquet",
     "fragrance_parquet/kcgpp100.parquet/part-00000-57b3a037-cd72-4bb0-8452-b99eb0159308-c000.snappy.parquet",
     "fragrance_parquet/kcrap100.parquet/part-00000-848d9410-faac-4685-9840-217106bbe90f-c000.snappy.parquet",
     "fragrance_parquet/kcrap200.parquet/part-00000-c09ec659-4cc9-441e-b2be-2e1b97d3e1d8-c000.snappy.parquet",
     "fragrance_parquet/mflbp100.parquet/part-00000-bb174455-fd45-44d4-89bd-ee159f0573a6-c000.snappy.parquet",
     "fragrance_parquet/mfltp100.parquet/part-00000-9df7cb45-6cdb-4b16-b2e2-9a4584c29097-c000.snappy.parquet",
     "fragrance_parquet/mftxp100.parquet/part-00000-92a75dcf-4177-49b1-a314-8d93ebf09ca6-c000.snappy.parquet",
     "fragrance_parquet/mfwop100.parquet/part-00000-dfbf539b-76b3-434d-b3b9-83b0fbcfac3e-c000.snappy.parquet",
     "fragrance_parquet/mfwop10h.parquet/part-00000-bbc992d9-aab7-40a9-bd45-b83389e2fe93-c000.snappy.parquet",
     "fragrance_parquet/mfwop300.parquet/part-00000-fbb5d784-aba5-4684-b252-72adabc3f033-c000.snappy.parquet",
     "fragrance_parquet/mfwop30h.parquet/part-00000-788abf56-1c2f-40b1-9264-aa4bd079d5e9-c000.snappy.parquet",
     "fragrance_parquet/mfwop400.parquet/part-00000-45b042d4-9fb9-4e22-9b02-c816e802b8aa-c000.snappy.parquet",
     "fragrance_parquet/mscmp100.parquet/part-00000-9eb0b1bf-b02e-49ba-acbe-5bda26e9e91d-c000.snappy.parquet",
     "fragrance_parquet/mspmp100.parquet/part-00000-ed79e79a-7d5c-4e00-ab83-27426242c5c5-c000.snappy.parquet",
     "fragrance_parquet/mspmpext.parquet/part-00000-af910dae-1852-4b3b-9328-6405b7efe0e1-c000.snappy.parquet",
     "fragrance_parquet/msvmp100.parquet/part-00000-28496ff5-dc2c-4d3b-8cae-dbda4e4ab82f-c000.snappy.parquet",
     "fragrance_parquet/obcdp100.parquet/part-00000-bf94a56b-f836-4b65-8ed2-a69f47c86cb0-c000.snappy.parquet",
     "fragrance_parquet/obcdp200.parquet/part-00000-cdbcf288-b5cd-4212-9705-956173d5ebd7-c000.snappy.parquet",
     "fragrance_parquet/obcop100.parquet/part-00000-0a7cd01e-4ad8-4f07-adb6-b5690187eb4b-c000.snappy.parquet",
     "fragrance_parquet/obcop200.parquet/part-00000-aa89b40d-fdd2-45ae-be10-4e3e18e0d31d-c000.snappy.parquet",
     "fragrance_parquet/obcop300.parquet/part-00000-3295bccf-e5ef-4297-90d7-b4fa5a10ae48-c000.snappy.parquet",
     "fragrance_parquet/obcopext.parquet/part-00000-7d661546-0815-40c2-8182-09bea2e09a23-c000.snappy.parquet",
     "fragrance_parquet/obcrp100.parquet/part-00000-9f72728c-1ffa-446b-965d-d2ece68aea57-c000.snappy.parquet",
     "fragrance_parquet/obcrpext.parquet/part-00000-9343d3f3-f683-4ae3-82b1-47e6790b37ec-c000.snappy.parquet",
     "fragrance_parquet/obirp111.parquet/part-00000-3ac04756-b9d0-4777-af8e-5e59c2c51323-c000.snappy.parquet",
     "fragrance_parquet/obotp100.parquet/part-00000-10fd0855-1827-4886-8333-d1b1e421046b-c000.snappy.parquet",
     "fragrance_parquet/obstp100.parquet/part-00000-5172f5bf-ee28-4bd6-aa4e-ef4bda1ff29b-c000.snappy.parquet",
     "fragrance_parquet/obtmp100.parquet/part-00000-2c71ed3a-753a-40da-8b03-46fc2a5c8e67-c000.snappy.parquet",
     "fragrance_parquet/obtop100.parquet/part-00000-8d96c73e-153f-43d3-8587-9dc6bbe46436-c000.snappy.parquet",
     "fragrance_parquet/popvp100.parquet/part-00000-c4d2f929-845a-4898-8705-48f13bca7dbc-c000.snappy.parquet",
     "fragrance_parquet/potmp100.parquet/part-00000-a779a6b3-e586-4abe-8d67-14e0801cac2c-c000.snappy.parquet",
     "fragrance_parquet/pspsp100.parquet/part-00000-97a73aa0-4fcb-45c9-95e5-5d90701c11f5-c000.snappy.parquet",
     "fragrance_parquet/sasmp100.parquet/part-00000-dccd937d-c6b6-42cd-8bdc-d41156a5db8f-c000.snappy.parquet"]

# The player is allowed to keep playing as long as their power is over 0.
while power < 52:
    print("You are still playing, because your power is %d." % power)

    print(l[power])

    buffer = io.BytesIO()
    s3 = boto3.resource('s3')
    s3_object = s3.Object('fragrancedatalake', 'FragranceAugust2017/glglp100.csv')
    s3_object.download_fileobj(buffer)
    table = pq.read_table(buffer)
    df = table.to_pandas()
    # df.iloc[0].to_csv(sys.stdout)
    df.iloc[0].to_csv(str(power)+".csv")

    power = power + 1


print("\nOh no, your power dropped to 0! Game Over.")

