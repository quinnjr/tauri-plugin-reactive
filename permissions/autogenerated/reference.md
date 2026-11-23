## Default Permission

Allows the windows this capability covers to subscribe to, read, write and push to every source the
app registers. Other windows are unaffected: the allow-everything scopes are command scopes, which
Tauri resolves per capability.

Restrict it by adding deny scopes, e.g. `{ "identifier": "reactive:allow-set", "deny": [{ "source": "settings" }] }`,
or build your own list from the `allow-*` command permissions plus `allow` scopes instead of this set.

#### This default permission set includes the following:

- `allow-subscribe-all-sources`
- `allow-get-all-sources`
- `allow-set-all-sources`
- `allow-sink-open-all-sources`
- `allow-unsubscribe`
- `allow-sink-push`
- `allow-sink-close`
- `allow-sink-abort`

## Permission Table

<table>
<tr>
<th>Identifier</th>
<th>Description</th>
</tr>


<tr>
<td>

`reactive:allow-get`

</td>
<td>

Enables the get command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-get`

</td>
<td>

Denies the get command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-set`

</td>
<td>

Enables the set command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-set`

</td>
<td>

Denies the set command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-sink-abort`

</td>
<td>

Enables the sink_abort command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-sink-abort`

</td>
<td>

Denies the sink_abort command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-sink-close`

</td>
<td>

Enables the sink_close command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-sink-close`

</td>
<td>

Denies the sink_close command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-sink-open`

</td>
<td>

Enables the sink_open command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-sink-open`

</td>
<td>

Denies the sink_open command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-sink-push`

</td>
<td>

Enables the sink_push command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-sink-push`

</td>
<td>

Denies the sink_push command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-subscribe`

</td>
<td>

Enables the subscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-subscribe`

</td>
<td>

Denies the subscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-unsubscribe`

</td>
<td>

Enables the unsubscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:deny-unsubscribe`

</td>
<td>

Denies the unsubscribe command without any pre-configured scope.

</td>
</tr>

<tr>
<td>

`reactive:allow-subscribe-all-sources`

</td>
<td>

Allows subscribing to every registered source (in the windows this capability covers).

</td>
</tr>

<tr>
<td>

`reactive:allow-get-all-sources`

</td>
<td>

Allows reading every registered state (in the windows this capability covers).

</td>
</tr>

<tr>
<td>

`reactive:allow-set-all-sources`

</td>
<td>

Allows writing every registered writable state (in the windows this capability covers).

</td>
</tr>

<tr>
<td>

`reactive:allow-sink-open-all-sources`

</td>
<td>

Allows opening a writer to every registered sink (in the windows this capability covers).

</td>
</tr>
</table>
