"""Wrapper around the Pgvector vector database over VectorDB"""

import io
import logging
from contextlib import contextmanager
from typing import Any
from vespa.deployment import VespaDocker
from vespa.application import Vespa
from vespa.io import VespaResponse
from vespa.package import ApplicationPackage, RankProfile, Schema, Document, Field, FieldSet
from typing import Iterable
from ..api import VectorDB, DBCaseConfig

log = logging.getLogger(__name__) 

class VespaClient(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig,
        collection_name: str = "VespaCollection",
        drop_old: bool = False,
        **kwargs,
    ):
        self.name = "Vespa"
        self.db_config = db_config
        self.case_config = db_case_config
        self.table_name = collection_name
        self.dim = dim
        self.schema = Schema(name="mySchema",mode="index",
                                document=Document(
                                    fields=[
                                        Field(
                                                name="embedding",
                                                type="tensor<float>(x[1536])",
                                                indexing=["attribute", "summary"],
                                                attribute=["distance-metric: angular"],
                                            ),
                                    ],
                                ),
                            )
        self.vespa_application_package = ApplicationPackage(name="vespaBench")
        self.vespa_application_package.schema.add_fields(Field(
                                                name="embedding",
                                                type="tensor<float>(x[1536])",
                                                indexing=["attribute", "summary"],
                                                attribute=["distance-metric: angular"],
                                            ))
        self.vespa_application_package.schema.add_rank_profile(
                                                RankProfile(
                                                    name="default",
                                                    first_phase="closeness(field, embedding)",
                                                    inputs=[("query(query_embedding)", "tensor<float>(x[1536])")],
                                                )
        )
        # construct basic units
        vespa_docker = VespaDocker(container_image='vespaengine/vespa')
        self.conn = vespa_docker.deploy(application_package=self.vespa_application_package)
        
        if drop_old :
            log.info(f"Vespa client drop table : {self.table_name}")
            # self._drop_index()
            # self._drop_table()
            # self._create_table(dim)
            # self._create_index()
        
        # self.conn.close()
        self.conn = None

    @contextmanager
    def init(self) -> None:
        """
        Connect to Vespa Client
        """
        self.conn = Vespa(url = "http://127.0.0.1:8080/",application_package=self.vespa_application_package)
        # self.conn = self.conn
        # vespa_docker = VespaDocker(container_image='vespaengine/vespa')
        # self.conn = vespa_docker.deploy(application_package=self.vespa_application_package)
        
        try:
            yield
        finally:
            self.conn = None
    
    def _drop_table(self):
        assert self.conn is not None, "Connection is not initialized"
        pass
    
    def ready_to_load(self):
        pass

    def optimize(self):
        pass
    
    def _post_insert(self):
        log.info(f"{self.name} post insert before optimize")
        self._drop_index()
        self._create_index()

    def ready_to_search(self):
        pass
        
    def _drop_index(self):
        assert self.conn is not None, "Connection is not initialized"
        pass

    def vespa_feed(self, user:str, my_docs_to_feed:list) -> Iterable[dict]:
        doc_id = 0
        for doc in my_docs_to_feed:
            doc_id +=1
            yield {
                "fields": {
                    "embedding": doc
                        },
                "id": doc_id,
            }

    def callback(self, response:VespaResponse, id:str):
        if not response.is_successful():
            log.error(f"Document {id} failed to feed with status code {response.status_code}, url={response.url} response={response.json}")
    
    def _create_index(self):
        assert self.conn is not None, "Connection is not initialized"
        
        
    def _create_table(self, dim : int):
        assert self.conn is not None, "Connection is not initialized"
        
        try:
            # create table
            print("Create table here")
        except Exception as e:
            log.warning(f"Failed to create Vespa table: {self.table_name} error: {e}")
            raise e from None

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        **kwargs: Any,
    ) -> (int, Exception): # type: ignore
        assert self.conn is not None, "Connection is not initialized"

        try:
            self.conn.feed_iterable(iter=self.vespa_feed("", embeddings), callback=self.callback, max_queue_size=5000, max_workers=64, max_connections=128)
            return len(embeddings), None
        except Exception as e:
            log.warning(f"Failed to insert data into vsepa db ({self.table_name}), error: {e}")   
            return 0, e

    def search_embedding(        
        self,
        query: list[float],
        k: int = 100,
        filters: dict | None = None,
        timeout: int | None = None,
    ) -> list[int]:
        assert self.conn is not None, "Connection is not initialized"

        # search_param =self.case_config.search_param()
        approximate = True
        input_embedding_field="query_embedding"
        yql = "select * from sources * where "
        yql += f"{{targetHits: {k}, approximate: {approximate}}}"
        yql += f"nearestNeighbor('embedding', {input_embedding_field})"
        # if filter is not None:
        #     yql += f" and {filter}"
        # print(yql)
        query_param = {
            "yql": yql,
            f"input.query({input_embedding_field})": query,
            "ranking": 'default',
            "hits": k,
        }
        # print(query_param)
        response = self.conn.query(body=query_param)
        
        assert(response.is_successful())
        result = response.json["root"]["children"]
        ret = [data["fields"]["embedding"]["values"] for data in result]
        return ret
        
