/*
Copyright 2023.

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

package controllers

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	atv1alpha1 "github.com/dharmit/at-operator/api/v1alpha1"
)

// AtReconciler reconciles a At object
type AtReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=at.example.com,resources=ats,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=at.example.com,resources=ats/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=at.example.com,resources=ats/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create
// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *AtReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("==== Reconciling at ====")

	instance := &atv1alpha1.At{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// resource wasn't found, maybe it was deleted
			log.Info("at resource not found")
			return ctrl.Result{}, nil
		}
	}

	if instance.Status.Phase == "" {
		instance.Status.Phase = atv1alpha1.PhasePending
	}

	switch instance.Status.Phase {
	case atv1alpha1.PhasePending:
		log.Info("Phase: PENDING")
		diff, err := timeUntilSchedule(instance.Spec.Schedule)
		if err != nil {
			log.Error(err, "schedule parsing failed")
			return ctrl.Result{}, err
		}
		log.Info("Schedule parsing done", "Result", fmt.Sprintf("%v", diff))

		if diff > 0 {
			// wait until scheduled time
			return ctrl.Result{RequeueAfter: diff}, nil
		}

		log.Info("Time to execute", "Ready to execute", instance.Spec.Command)
		instance.Status.Phase = atv1alpha1.PhaseRunning
	case atv1alpha1.PhaseRunning:
		log.Info("Phase: RUNNING")
		pod := newPodForCR(instance)
		err := ctrl.SetControllerReference(instance, pod, r.Scheme)
		if err != nil {
			return ctrl.Result{}, err
		}

		query := &corev1.Pod{}
		// check if Pod already exists
		err = r.Get(ctx, req.NamespacedName, query)
		if err != nil && apierrors.IsNotFound(err) {
			// does not exist, create a Pod
			err = r.Create(ctx, pod)
			if err != nil {
				return ctrl.Result{}, err
			}

			log.Info("Pod created successfully", "name", pod.Name)
			return ctrl.Result{}, nil
		} else if err != nil {
			// requeue with err
			log.Error(err, "could not create pod")
			return ctrl.Result{}, err
		} else if query.Status.Phase == corev1.PodFailed ||
			query.Status.Phase == corev1.PodSucceeded {
			// pod finished or error'd out
			log.Info("Container terminated", "reason", query.Status.Reason,
				"message", query.Status.Message)
			instance.Status.Phase = atv1alpha1.PhaseDone
		} else {
			// don't requeue, it will happen automatically when
			// pod status changes
			return ctrl.Result{}, nil
		}
	case atv1alpha1.PhaseDone:
		log.Info("Phase: DONE")
		// reconcile without requeueing
		return ctrl.Result{}, err
	default:
		log.Info("NOP")
		return ctrl.Result{}, err
	}

	// update status
	err = r.Status().Update(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func timeUntilSchedule(schedule string) (time.Duration, error) {
	now := time.Now().UTC()
	layout := "2006-01-02T15:04:05Z"
	scheduledTime, err := time.Parse(layout, schedule)
	if err != nil {
		return time.Duration(0), err
	}
	return scheduledTime.Sub(now), nil
}

func newPodForCR(cr *atv1alpha1.At) *corev1.Pod {
	labels := map[string]string{"app": cr.Name}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "busybox",
					Image:   "quay.io/quay/busybox",
					Command: strings.Split(cr.Spec.Command, " "),
				},
			},
			RestartPolicy: corev1.RestartPolicyOnFailure,
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *AtReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&atv1alpha1.At{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
