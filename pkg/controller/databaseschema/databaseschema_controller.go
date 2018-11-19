/*
Copyright 2018 Dirk Jablonski.

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

package databaseschema

import (
	"context"
	"database/sql"
	"log"
	"reflect"

	_ "github.com/go-sql-driver/mysql"

	databasev1alpha1 "k8s-db-controller/pkg/apis/database/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new DatabaseSchema Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
// USER ACTION REQUIRED: update cmd/manager/main.go to call this database.Add(mgr) to install this Controller
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileDatabaseSchema{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("databaseschema-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to DatabaseSchema
	err = c.Watch(&source.Kind{Type: &databasev1alpha1.DatabaseSchema{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create
	// Uncomment watch a Deployment created by DatabaseSchema - change this for objects you create
	err = c.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &databasev1alpha1.DatabaseSchema{},
	})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileDatabaseSchema{}

// ReconcileDatabaseSchema reconciles a DatabaseSchema object
type ReconcileDatabaseSchema struct {
	client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a DatabaseSchema object and makes changes based on the state read
// and what is in the DatabaseSchema.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  The scaffolding writes
// a Deployment as an example
// Automatically generate RBAC rules to allow the Controller to read and write Deployments
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=database.example.org,resources=databaseschemas,verbs=get;list;watch;create;update;patch;delete
func (r *ReconcileDatabaseSchema) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the DatabaseSchema instance
	instance := &databasev1alpha1.DatabaseSchema{}
	err := r.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// TODO(user): Change this to be the object type created by your controller
	// Define the desired Secret object
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name + "-database-secret",
			Namespace: instance.Namespace,
		},
		Data: map[string][]byte{
			"database.name": []byte(instance.Spec.SchemaName),
		},
	}
	if err := controllerutil.SetControllerReference(instance, secret, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	// TODO(user): Change this for the object type created by your controller
	// Check if the Secret already exists
	found := &corev1.Secret{}
	err = r.Get(context.TODO(), types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		log.Printf("Creating Secret %s/%s\n", secret.Namespace, secret.Name)
		err = r.Create(context.TODO(), secret)
		if err != nil {
			return reconcile.Result{}, err
		}

		// Generate database in MySQL
		err := createDatabase(instance.Spec.SchemaName)
		if err != nil {
			log.Printf("Could not create database: %s", err)
			return reconcile.Result{}, nil
		}
	} else if err != nil {
		return reconcile.Result{}, err
	}

	// TODO(user): Change this for the object type created by your controller
	// Update the found object and write the result back if there are any changes
	if !reflect.DeepEqual(secret.Data, found.Data) {
		found.Name = secret.Name
		found.Namespace = secret.Namespace
		found.Data = secret.Data
		found.ObjectMeta = secret.ObjectMeta
		log.Printf("Updating Secret %s/%s\n", secret.Namespace, secret.Name)
		err = r.Update(context.TODO(), found)
		if err != nil {
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{}, nil
}

func createDatabase(name string) error {
	db, err := sql.Open("mysql", "root:root@tcp(mysql.default.svc.cluster.local)/mysql")
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + name)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return err
	} else {
		log.Printf("Database %s successfully created.\n", name)
		return nil
	}
}
