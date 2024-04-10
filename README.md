# IoMBian Updatable Services Handler

This services is in charge of updating the updatable services field of the firestore device.

`updatable_services` is a field on each device that determines if a service can be updated.
If there is a new version available of an installed service, it is represented like this:
```
{
    ...,
    updatable_services: {
        iombian-button-handler: "0.1.1"
    }
}
```

Whenever a new service version is added or the installed service version is updated, the updatable services changes.

## Author
(c) 2024 IoMBian team ([Aitor Iturrioz Rodríguez](https://github.com/bodiroga), [Aitor Castaño Mesa](https://github.com/aitorcas23)).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
