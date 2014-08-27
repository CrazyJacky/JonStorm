package sheng;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;


/**
 * Created by shezhao on 8/26/14.
 */
public class SingleKafkaSpout extends KafkaSpout {
  public SingleKafkaSpout(SpoutConfig spoutConf) {
    super(spoutConf);
  }

//  @Override
//  public void nextTuple() {
//    List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
//    for (int i = 0; i < managers.size(); i++) {
//
//      try {
//        // in case the number of managers decreased
//        _currPartitionIndex = _currPartitionIndex % managers.size();
//        EmitState state = managers.get(_currPartitionIndex).next(_collector);
//        if (state != EmitState.EMITTED_MORE_LEFT) {
//          _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
//        }
//        if (state != EmitState.NO_EMITTED) {
//          break;
//        }
//      } catch (FailedFetchException e) {
//        LOG.warn("Fetch failed", e);
//        _coordinator.refresh();
//      }
//    }
//
//    long now = System.currentTimeMillis();
//    if ((now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
//      commit();
//    }
//  }


}
