/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package transfer

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/metrics"
)

// Controller is controller that enables transfer of a pvc
type Controller struct {
	client clientset.Interface

	pvcLister       corelisters.PersistentVolumeClaimLister
	pvLister        corelisters.PersistentVolumeLister
	pvcListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

// NewTransferController returns a new instance of TransferController.
func NewTransferController(pvcInformer coreinformers.PersistentVolumeClaimInformer, pvInformer coreinformers.PersistentVolumeInformer, cl clientset.Interface) *Controller {
	e := &Controller{
		client: cl,
		queue:  workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "pvctransfer"),
	}
	if cl != nil && cl.CoreV1().RESTClient().GetRateLimiter() != nil {
		metrics.RegisterMetricAndTrackRateLimiterUsage("transfer_controller", cl.CoreV1().RESTClient().GetRateLimiter())
	}

	e.pvcLister = pvcInformer.Lister()
	e.pvLister = pvInformer.Lister()
	pvcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, new interface{}) {
			e.pvcAddedUpdated(new)
		},
	})

	return e
}

// Run runs the controller goroutines.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.V(1).Infof("Starting PVC transfer controller")
	defer glog.V(1).Infof("Shutting down PVC transfer controller")
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

// runWorker is our goroutine, we launch one of these for each "workers" and just loop on processNextWorkItem
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one pvcKey off the queue.  It returns false when it's time to quit.
// we just loop on this function pulling an item of the queue, and when we have one send it to processPVC
func (c *Controller) processNextWorkItem() bool {
	pvcKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(pvcKey)

	pvcNamespace, pvcName, err := cache.SplitMetaNamespaceKey(pvcKey.(string))
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Error parsing PVC key %q: %v", pvcKey, err))
		return true
	}

	err = c.processPVC(pvcNamespace, pvcName)
	if err == nil {
		c.queue.Forget(pvcKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("PVC %v failed with : %v", pvcKey, err))
	c.queue.AddRateLimited(pvcKey)

	return true
}

// pvcAddedUpdated reacts to pvc updated events from the system by adding them to the queue if they require processing
func (c *Controller) pvcAddedUpdated(obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PVC informer returned non-PVC object: %#v", obj))
		return
	}
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(pvc)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for Persistent Volume Claim %#v: %v", pvc, err))
		return
	}
	if metav1.HasAnnotation(pvc.ObjectMeta, "transfer/destination-ns") {
		glog.V(1).Infof("Adding PVC to transfer queue %s", key)
		c.queue.Add(key)
	} else {
		glog.V(1).Infof("Skip adding to transfer queue as there's no annotation: %s", key)
	}

}

// processPVC does our actual "work"
func (c *Controller) processPVC(pvcNamespace, pvcName string) error {
	glog.V(1).Infof("Processing PVC %s/%s", pvcNamespace, pvcName)
	startTime := time.Now()
	defer func() {
		glog.V(1).Infof("Finished processing PVC %s/%s (%v)", pvcNamespace, pvcName, time.Since(startTime))
	}()

	pvc, err := c.pvcLister.PersistentVolumeClaims(pvcNamespace).Get(pvcName)
	glog.V(1).Infof("pulled a pvc: %s", pvc)
	if apierrs.IsNotFound(err) {
		glog.V(1).Infof("PVC %s/%s not found, ignoring", pvcNamespace, pvcName)
		return nil
	}
	if err != nil {
		return err
	}
	updatedPVC := pvc.DeepCopy()
	updatedPVC.Annotations["I-C-U"] = "runnin"
	_, err = c.client.CoreV1().PersistentVolumeClaims(updatedPVC.Namespace).Update(updatedPVC)
	if err != nil {
		glog.V(1).Infof("Awwww nuts!  Update failed: %s", err.Error())
	}

	return nil
}
