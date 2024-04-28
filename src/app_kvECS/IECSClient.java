package app_kvECS;

import java.util.Map;
import java.util.Collection;

import ecs.IECSNode;

public interface IECSClient {
    /**
     * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
     * @throws Exception    some meaningfull exception on failure
     * @return  true on success, false on failure
     */
    public boolean start() throws Exception;

}
