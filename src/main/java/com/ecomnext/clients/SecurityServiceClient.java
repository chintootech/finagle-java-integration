package com.ecomnext.clients;

import com.ecomnext.domain.TDataAccessException;
import com.ecomnext.domain.TNotFoundException;
import com.ecomnext.domain.User;
import com.ecomnext.domain.exceptions.*;
import com.ecomnext.services.SecurityService;
import com.ecomnext.util.ListTransform;
import com.ecomnext.util.ScalaSupport;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.service.Backoff;
import com.twitter.finagle.service.RetryBudget;
import com.twitter.finagle.service.RetryBudgets;
import com.twitter.finagle.stats.InMemoryStatsReceiver;
import com.twitter.finagle.thrift.service.ThriftResponseClassifier;
import com.twitter.util.Duration;
import com.twitter.util.TimeoutException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client for {@link SecurityService}.
 */
public class SecurityServiceClient {
    //Service<ThriftClientRequest,byte[]> service;
    SecurityService.FutureIface client;

    /**
     * @param connectionLimit maximum number of connections from the client to the host.
     */
    public SecurityServiceClient(String host, int port, int connectionLimit) {
        /*service = ClientBuilder.safeBuild(
                ClientBuilder.get()
                        .hosts(new InetSocketAddress(host, port))
                        .codec(ThriftClientFramedCodec.get())
                        .hostConnectionLimit(connectionLimit)
        );

        client = new SecurityService.FinagledClient(
                service,
                new TBinaryProtocol.Factory(),
                "MerchantsService",
                new InMemoryStatsReceiver());*/

        Duration ttl = Duration.apply(10, TimeUnit.SECONDS);
        Integer minRetriesPerSec = 5;
        Double maxPercentOver = 0.1;

        RetryBudget budget = RetryBudgets.newRetryBudget(ttl, minRetriesPerSec, maxPercentOver);

        //#thriftclientapi
        client = Thrift.client()
            //.withMonitor(monitor)
            .withLabel("Thrift Client")
            //.withTransport().verbose()
            .withSessionPool().minSize(1) //10
            .withSessionPool().maxSize(5) //20
            .withSessionPool().maxWaiters(100)
            .withResponseClassifier(ThriftResponseClassifier.ThriftExceptionsAsFailures())
            .withRetryBudget(budget)
            .withRetryBackoff(Backoff.constant(Duration.apply(10, TimeUnit.SECONDS)))
            .withStatsReceiver(new InMemoryStatsReceiver())
            .newIface(String.format("%s:%d", host, port), SecurityService.FutureIface.class);
    }

    public void close() {
        //service.close();
    }

    public User getUser(String username, long timeout) {
        try {
            return new User(
                    client.getUser(username)
                            .apply(
                                    new Duration(TimeUnit.MILLISECONDS.toNanos(timeout))
                            )
            );
        } catch (Exception e) {
            if (e instanceof ServiceException) {
                throw (ServiceException) e;
            } else if (e instanceof TimeoutException) {
                throw new ServiceTimeoutException(e);
            } else if (e instanceof TApplicationException) {
                throw new ServiceRemoteException(((TApplicationException)e).getType(), e.getMessage());
            } else if (e instanceof TTransportException) {
                throw new ServiceTransportException(e);
            } else if (e instanceof TNotFoundException) {
                return null;
            } else if (e instanceof TDataAccessException) {
                throw new ServiceDataAccessException(e);
            } else {
                throw new ServiceException(e);
            }
        }
    }

    public List<User> getUsers(long timeout) {
        try {
            return ListTransform.transform(
                    ScalaSupport.toJavaList(
                            client.getUsers()
                                    .apply(
                                            new Duration(TimeUnit.MILLISECONDS.toNanos(timeout))
                                    )
                    ), input -> new User(input));
        } catch (Exception e) {
            if (e instanceof ServiceException) {
                throw (ServiceException) e;
            } else if (e instanceof TimeoutException) {
                throw new ServiceTimeoutException(e);
            } else if (e instanceof TApplicationException) {
                throw new ServiceRemoteException(((TApplicationException)e).getType(), e.getMessage());
            } else if (e instanceof TTransportException) {
                throw new ServiceTransportException(e);
            } else if (e instanceof TNotFoundException) {
                throw new ServiceNotFoundException(e);
            } else if (e instanceof TDataAccessException) {
                throw new ServiceDataAccessException(e);
            } else {
                throw new ServiceException(e);
            }
        }
    }

    public int countUsersByRole(boolean enabled, List<String> roles, long timeout) {
        try {
            return (Integer) client.countUsersByRole(enabled, ScalaSupport.toScalaSeq(roles))
                            .apply(
                                    new Duration(TimeUnit.MILLISECONDS.toNanos(timeout))
                            );
        } catch (Exception e) {
            if (e instanceof ServiceException) {
                throw (ServiceException) e;
            } else if (e instanceof TimeoutException) {
                throw new ServiceTimeoutException(e);
            } else if (e instanceof TApplicationException) {
                throw new ServiceRemoteException(((TApplicationException)e).getType(), e.getMessage());
            } else if (e instanceof TTransportException) {
                throw new ServiceTransportException(e);
            } else if (e instanceof TNotFoundException) {
                throw new ServiceNotFoundException(e);
            } else if (e instanceof TDataAccessException) {
                throw new ServiceDataAccessException(e);
            } else {
                throw new ServiceException(e);
            }
        }
    }

    public void cleanUpOldUsers(long timeout) {
        try {
            client.cleanUpOldUsers().apply(new Duration(TimeUnit.MILLISECONDS.toNanos(timeout)));
        } catch (Exception e) {
            if (e instanceof ServiceException) {
                throw (ServiceException) e;
            } else if (e instanceof TimeoutException) {
                throw new ServiceTimeoutException(e);
            } else if (e instanceof TApplicationException) {
                throw new ServiceRemoteException(((TApplicationException)e).getType(), e.getMessage());
            } else if (e instanceof TTransportException) {
                throw new ServiceTransportException(e);
            } else if (e instanceof TNotFoundException) {
                throw new ServiceNotFoundException(e);
            } else if (e instanceof TDataAccessException) {
                throw new ServiceDataAccessException(e);
            } else {
                throw new ServiceException(e);
            }
        }
    }
}
