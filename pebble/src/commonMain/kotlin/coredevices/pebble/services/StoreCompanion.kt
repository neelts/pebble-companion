package coredevices.pebble.services

import co.touchlab.kermit.Logger
import coredevices.database.AppstoreSourceDao
import io.rebble.libpebblecommon.connection.ConnectedPebbleDevice
import io.rebble.libpebblecommon.connection.LibPebble
import io.rebble.libpebblecommon.di.LibPebbleCoroutineScope
import io.rebble.libpebblecommon.services.appmessage.AppMessageData
import io.rebble.libpebblecommon.services.appmessage.AppMessageResult
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import org.koin.core.parameter.parametersOf
import kotlin.uuid.Uuid

/**
 * pebble-store: lets our on-watch store app drive installs from the wrist.
 *
 * Subscribes to AppMessages from our store watch-app UUID on every connected
 * watch. On an "install" command it resolves the app from the Pebble appstore
 * and installs it through the proper locker path (icon-correct + account-tracked)
 * via [LibPebble.addAppToLocker] — no napp, no developer connection.
 */
class StoreCompanion(
    private val libPebble: LibPebble,
    private val appstoreSourceDao: AppstoreSourceDao,
    private val scope: LibPebbleCoroutineScope,
) : KoinComponent {
    private val logger = Logger.withTag("StoreCompanion")
    // Mutated only from the single watches-collector coroutine below → no locking needed.
    private val subscribed = mutableSetOf<String>()

    fun init() {
        logger.d { "init()" }
        scope.launch {
            libPebble.watches.collect { devices ->
                val connected = devices.filterIsInstance<ConnectedPebbleDevice>()
                subscribed.retainAll(connected.map { it.identifier.toString() }.toSet())
                for (device in connected) {
                    if (subscribed.add(device.identifier.toString())) {
                        scope.launch {
                            device.inboundAppMessages(STORE_UUID).collect { msg -> handle(device, msg) }
                        }
                    }
                }
            }
        }
    }

    private suspend fun handle(device: ConnectedPebbleDevice, msg: AppMessageData) {
        device.sendAppMessageResult(AppMessageResult.ACK(msg.transactionId))
        val cmd = (msg.data[KEY_COMMAND] as? String)?.trim() ?: return
        val id = (msg.data[KEY_ITEM_ID] as? String)?.trim()
        logger.d { "store cmd='$cmd' id='$id'" }
        when (cmd) {
            "install" -> if (id.isNullOrEmpty()) reply(device, "err:noid") else install(device, id)
        }
    }

    private suspend fun install(device: ConnectedPebbleDevice, id: String) {
        val sources = appstoreSourceDao.getAllSources().first()
        if (sources.isEmpty()) {
            reply(device, "err:nosrc"); return
        }
        // The watch's ids come from whichever feed served browse (Rebble via napp
        // today), so try every source — Rebble first — until one resolves the id.
        for (source in sources.sortedByDescending { it.isRebbleFeed() }) {
            val storeApp = runCatching {
                get<AppstoreService> { parametersOf(source) }
                    .fetchAppStoreApp(id, hardwarePlatform = null)
                    ?.data?.firstOrNull()
            }.getOrElse { logger.e(it) { "fetch from ${source.url} failed" }; null } ?: continue
            val entry = storeApp.toLockerEntry(source.url, null) ?: continue
            runCatching { libPebble.addAppToLocker(entry) }
                .onSuccess { logger.d { "installed $id (${storeApp.title}) from ${source.url}" }; reply(device, "installed") }
                .onFailure { logger.e(it) { "install failed" }; reply(device, "err:install") }
            return
        }
        reply(device, "err:notfound")
    }

    private suspend fun reply(device: ConnectedPebbleDevice, status: String) {
        runCatching {
            device.sendAppMessage(
                AppMessageData(
                    transactionId = device.transactionSequence.next(),
                    uuid = STORE_UUID,
                    data = mapOf(KEY_STATUS to status),
                )
            )
        }
    }

    companion object {
        // Our pebble-store watch app UUID + numeric AppMessage keys (base 10000 + index).
        val STORE_UUID: Uuid = Uuid.parse("4ad7ea3c-9a53-4ed7-9e6f-000cdbd08c0c")
        const val KEY_COMMAND = 10000 // "install"
        const val KEY_ITEM_ID = 10004 // appstore id
        const val KEY_STATUS = 10011  // reply status string
    }
}
