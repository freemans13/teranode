
 ~  $ aws configure
AWS Access Key ID [None]: ...
AWS Secret Access Key [None]: ....
Default region name [None]: eu-north-1
Default output format [None]:

 ~  $ aws eks update-kubeconfig --name aws-ubsv-playground --region ap-south-1
Added new context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground to $HOME/.kube/config

aws eks update-kubeconfig --name aws-ubsv-playground --region  eu-west-1
aws eks update-kubeconfig --name aws-ubsv-playground --region  us-east-1
aws eks update-kubeconfig --name aws-ubsv-playground --region  ap-south-1

this stuff is now under your
~/.kube  $ vi config
 ~/.kube  $

brew install kubectl

kubectl get pod


Adding under .zprofile:

# Load k8s shortcuts
autoload -Uz compinit
compinit
source ~/.k8s_shortcuts.sh


Docker is defined in the Dockerfile. This is what we run in the k8s

main.go --> os.Args(0) --> run a command.
We deploy the compiled binary once, they all run the same image. Kubernetes rules below will define what Dockers to run

(see keti blockassembly-12312321 --bash) --> ls -l --> we have all the runs there.
The ubsv.run --> is the binary that is run in the Docker image. We just have symlinks.



---

Settings for M1 and M2 and other envs are in the settings.
see settings with scaling.M1 suffixes.

kcn m1 -- this switches me from one to another

--- For runthrough with simon, we used utxo-blaster.yaml

We work with the utxo-blaster.yaml which exists per env or node in the examples below

In the utxo-blaster.yaml we have the following:

        - Name is important
        - replicas: 0 - we want to have zero instances (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        - role: propagation - only the pods with that role, run on those machines
        - tolerations --> ??
        - image: REPO|IMAGE|TAG  (this will be the ubsv.run)
        - command - command to run in the dockerimage, else it runs the default (entry point)
                -- and args on top of it
        -- resources - machine requirements (otherwise do not run it)
                        (requests vs limits - sets a min and a max)
        -- readinessProbe -- is the pod ready to run?
        -- livenessProbe -- is the pod alive? In our case, we use both the same (health checks)
           we can test this by doing curl http://m1.scaling.ubsv.dev:9091/health
        -- volumeMounts - we can mount volumes. there is a claimName, which links to the type -- like for example "claimName: luster-pvc" will go to luster-pvc.yaml which is an example of persistence storage. Here we can define sizes, and so forth.
        --

Roles can be seen as
k get node ### all AWS machines in a given zone (node)
k get node -L role

In the CI, we can do:

kustomize kustomization.yaml ## runs as part of the CI build --> deploy-to-region.yaml
- running this command - https://capture.dropbox.com/MYdvV14jbXe46Tym - it adds a images block at the end of the kustomization.yaml (during CI) - we do not want to commit this.
- kustomize build . # populates all of the k8s yaml files and replaces the image and other stuff.

We have a utxo-blaster-service.yaml which is the service that runs in the k8s cluster. It is a load balancer that points to the pods that are running the utxo-blaster.yaml
It has a ports definition that points to the port and the target port that the pods are running.

asset-grpc-ingress.yaml - it shows the ingress (the way to get to the service from the outside world)
ast-grpc - basically means grpc over port 80


Host('m.scaling.usbv.dev')

In the regional k8s folder, we have further kustomization.yaml that further customise per region. For example, it appends an 1 at the end of service  names in the M1 env, 2 in the M2 env, and so on. Also replace the spec names to Host('m1.scaling.usbv.dev') and so on.


----

Questions:

Do services need to be started in any specific order? YES -- see ubsv/scripts/k8s/scaling/up.sh

* Blockchain server
* asset
* etc

Most critical appears to be the Blockchain Server.

Block Assembly Service - if it restarts, it loses all transactions in memory.

Interesting to test the stuff Sukhendu is doing:
docker-compose up postgres
docker-compose-ci-distributed.yaml ubsv-1 (starts one of hte services)


---

Simon - How do I change settings locally?


ked block-assembly-213131 -- vi block-assembly.run (name might not be exact)

after saving it --> Kubernetes restarts the service automatically

----


autoload -Uz compinit
compinit
source ~/.k8s_shortcuts.sh

alias m1='kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground; kcn m1'



alias m2='kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground; kcn m2'



alias m3='kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground; kcn m3'


-----


kgp --- see all current services running in a given node (Kubernetes running Docker nodes)

----

Apologies, but what command can I run to reset p2p bootstrap server so that m2 p2p service starts working again? I’ve tried stopping and starting p2pbootstrap but that didn’t do anything

k delete pod -n p2pbootstrap --all

in each even

then same for p2p service in m1 and m2

k delete pod p2p1…

----
kpg -n traefik


---

kgpa - lists all services


kubectl top pods

or periodically
watch -3 kubectl top pods


start coinbase in M1 and M2

ksd coinbase1 --replicas=1 (run in each server)

----

start miner in M2

ksd miner2 --replicas=1

----

ksd propagation1 --replicas=13


----
ksd tx-blaster1 --replicas=11 # should be run in M1 only

---

k get node -L role | grep prop | wc -l ### check number of instances of a service in a node

---

kl tx-blaster1-xxxxxx (whatever name appears in the kgp command)

-----

keti blockassembly1-234234-kpkvr ---bash
mount

---


k get pv ### shows persistent storage
        --> CLAIM label--> m1/lustre-pv
k describe pvc subtree-lustre-pv

asset.yaml --> lustre is in there.


---

Forward asset server to my localhost

export KUBECONFIG=~/.kube/config-m1

kubectl port-forward asset1-63453452352-23452:8090:8090

(remember ports are defined in the settings local file)


-----


Grafana stuff - it can be linked here for a way to check stuff out
