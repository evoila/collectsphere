import time
import collectsphere
from collectd import Conf
import sys   
 
def main():
    
    children = []
    children.append(Conf('Name', ('vc47-1',)))
    children.append(Conf('Host', ('vc47-1.mobile.rz',)))
    children.append(Conf('Port', ('443',)))
    children.append(Conf('Verbose', ('True',)))
    children.append(Conf('Username', ('autoconf',)))
    children.append(Conf('Password', ('4b0dac37',)))
    children.append(Conf('Host_Counters', ('cpu.usage,cpu.ready,net.bytesTx,net.bytesRx,net.droppedRx,net.droppedTx,mem.usage,mem.swapused,mem.consumed,mem.compressed,mem.vmmemctl,disk.write,disk.totalLatency,disk.usage,disk.read,datastore.datastoreIops,disk.kernelLatency',)))
    children.append(Conf('VM_Counters', ('',)))
    children.append(Conf('Inventory_Refresh_Interval', ('600',)))
        
    conf = Conf(None, None, children)
    
    start_time = time.time()
    collectsphere.configure_callback(conf)
    collectsphere.init_callback()
    elapsed_time = time.time() - start_time      
    sys.stderr.write("Conf Time: " + str(elapsed_time) + "\n")    

    while True:
        start_time = time.time()        
        collectsphere.read_callback()
        elapsed_time = time.time() - start_time      
        sys.stderr.write("Read Time: " + str(elapsed_time) + "\n")
        time.sleep(20)

if __name__ == "__main__":
    main()
