"""Holds the base object EdgeChecker to be implemented and used in other classes that identify edges."""

#  Copyright (c) NCC Group and Erik Steringer 2019. This file is part of Principal Mapper.
#
#      Principal Mapper is free software: you can redistribute it and/or modify
#      it under the terms of the GNU Affero General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      Principal Mapper is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU Affero General Public License for more details.
#
#      You should have received a copy of the GNU Affero General Public License
#      along with Principal Mapper.  If not, see <https://www.gnu.org/licenses/>.

import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional, Union

import botocore.session

from principalmapper.common import Edge, Node
from principalmapper.util import split_list


class EdgeChecker(object):
    """Base class for all edge-identifying classes."""

    def __init__(self, session: Union[botocore.session.Session, None]):
        self.session = session

    def _parallelize(
        self,
        nodes: List[Node],
        logger: logging.Logger,
        func: Callable[..., List[Edge]],
        /,
        *args,
        **kwargs,
    ) -> List[Edge]:
        """Helper method to parallelize edge generation. Splits the list of nodes into smaller chunks and processes
        them in parallel using ThreadPoolExecutor. First parameter of func is always a list of nodes to process
        (List[Node]), followed by any other parameters passed in *args and **kwargs."""
        thread_count = max(8, (os.cpu_count() or 1) + 4)
        buckets = split_list(
            # Split the list of nodes into smaller chunks for processing
            nodes,
            thread_count,
        )

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = {
                executor.submit(func, bucket, *args, **kwargs): bucket
                for bucket in buckets
            }
            result = []
            for future in as_completed(futures):
                try:
                    result.extend(future.result())
                except Exception as exc:
                    logger.error(
                        "Generated an exception during Lambda edge generation: {}".format(
                            exc
                        )
                    )
                    logger.debug("Exception details: {}".format(exc), exc_info=True)
                    continue
        return result

    def return_edges(
        self,
        nodes: List[Node],
        region_allow_list: Optional[List[str]] = None,
        region_deny_list: Optional[List[str]] = None,
        scps: Optional[List[List[dict]]] = None,
        client_args_map: Optional[dict] = None,
    ) -> List[Edge]:
        """Subclasses shall override this method. Given a list of nodes, the EdgeChecker should be able to use its session
        object in order to make clients and call the AWS API to resolve information about the account. Then,
        with this information, it should return a list of edges between the passed nodes.

        The region allow/deny lists are mutually-exclusive (i.e. at least one of which has the value None) lists of
        allowed/denied regions to pull data from.
        """
        raise NotImplementedError(
            "The return_edges method should not be called from EdgeChecker, but rather from an "
            "object that subclasses EdgeChecker"
        )
