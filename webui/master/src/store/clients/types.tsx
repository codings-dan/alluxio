/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import { AxiosResponse } from 'axios';

import { IClientNodeInfo } from '../../constants';

export interface IClients {
  normalClientNodeInfos: IClientNodeInfo[];
  failedClientNodeInfos: IClientNodeInfo[];
}

export enum ClientsActionTypes {
  FETCH_REQUEST = '@@clients/FETCH_REQUEST',
  FETCH_SUCCESS = '@@clients/FETCH_SUCCESS',
  FETCH_ERROR = '@@clients/FETCH_ERROR',
}

export interface IClientsState {
  readonly data: IClients;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
