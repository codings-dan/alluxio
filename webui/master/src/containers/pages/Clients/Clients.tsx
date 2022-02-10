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

import React from 'react';
import { connect } from 'react-redux';
import { Table } from 'reactstrap';
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withLoadingMessage, withFetchData } from '@alluxio/common-ui/src/components';
import { IClientNodeInfo } from '../../../constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/clients/actions';
import { IInit } from '../../../store/init/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';
import { IClients } from '../../../store/clients/types';

interface IPropsFromState extends ICommonState {
  initData: IInit;
  clientsData: IClients;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class ClientsPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { clientsData } = this.props;

    return (
      <div className="clients-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Live Clients</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    <th>Node Name(Container Host)</th>
                    <th>PID</th>
                    <th>Client Id</th>
                    <th>Last Heartbeat</th>
                    <th>Client Metadata Cache Size</th>
                    <th>Up time</th>
                  </tr>
                </thead>
                <tbody>
                  {clientsData.normalClientNodeInfos.map((nodeInfo: IClientNodeInfo) => (
                    <tr key={nodeInfo.clientId}>
                      <td>
                        <a
                          id={`id-${nodeInfo.clientId}-link`}
                          href={
                            // When clients start with kubernetes. `nodeInfo.host` is `hostIp (podIp)`
                            nodeInfo.host.includes('(')
                              ? `//${nodeInfo.host.substring(0, nodeInfo.host.indexOf('(')).trim()}`
                              : `//${nodeInfo.host}`
                          }
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          {nodeInfo.host}
                        </a>
                      </td>
                      <th>{nodeInfo.pid}</th>
                      <td>{nodeInfo.clientId}</td>
                      <td>{nodeInfo.lastHeartbeat}</td>
                      <td>{nodeInfo.metadataCacheSize}</td>
                      <td>{nodeInfo.registerTime}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </div>
          </div>
          <div className="row">
            <div className="col-12">
              <h5>Lost Clients</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    <th>Node Name(Container Host)</th>
                    <th>PID</th>
                    <th>Clients Id</th>
                    <th>Last Heartbeat</th>
                  </tr>
                </thead>
                <tbody>
                  {clientsData.failedClientNodeInfos.map((nodeInfo: IClientNodeInfo) => (
                    <tr key={nodeInfo.clientId}>
                      <td>
                        <a
                          id={`id-${nodeInfo.clientId}-link`}
                          href={
                            // When clients start with kubernetes. `nodeInfo.host` is `hostIp (podIp)`
                            nodeInfo.host.includes('(')
                              ? `//${nodeInfo.host.substring(0, nodeInfo.host.indexOf('(')).trim()}`
                              : `//${nodeInfo.host}`
                          }
                          rel="noopener noreferrer"
                          target="_blank"
                        >
                          {nodeInfo.host}
                        </a>
                      </td>
                      <td>{nodeInfo.pid}</td>
                      <td>{nodeInfo.clientId}</td>
                      <td>{nodeInfo.lastHeartbeat}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({ init, refresh, clients }: IApplicationState): IPropsFromState => ({
  initData: init.data,
  errors: createAlertErrors(init.errors !== undefined || clients.errors !== undefined),
  loading: init.loading || clients.loading,
  refresh: refresh.data,
  clientsData: clients.data,
  class: 'clients-page',
});

const mapDispatchToProps = (dispatch: Dispatch): { fetchRequest: () => AnyAction } => ({
  fetchRequest: (): AnyAction => dispatch(fetchRequest()),
});

export default compose(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  ),
  withFetchData,
  withErrors,
  withLoadingMessage,
)(ClientsPresenter) as typeof React.Component;
