package raft

import raft.Message.*
import java.util.LinkedList
import java.util.Queue

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Daniil Eshkin
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    private var commitIndex: Int = 0
    private var lastApplied: Int = 0

    private var nextIndex: Array<Int> = Array(env.nProcesses + 1) { 0 }
    private var matchIndex: Array<Int> = Array(env.nProcesses + 1) { 0 }

    private var state: State = State.FOLLOWER
    private var votesCnt: Int = 0
    private var leaderId: Int? = null

    private val commands: Queue<Command> = LinkedList()

    private var nuSubsequentTakSubsequent = true

    init {
        becomeFollower()
    }

    private fun curTerm(): Int {
        return storage.readPersistentState().currentTerm
    }

    private fun votedFor(): Int? {
        return storage.readPersistentState().votedFor
    }

    private fun startElection() {
        state = State.CANDIDATE
        storage.writePersistentState(PersistentState(curTerm() + 1, env.processId))

        votesCnt = 1
        leaderId = null

        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                env.send(i, RequestVoteRpc(curTerm(), storage.readLastLogId()))
            }
        }

        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun sendHeartbeats() {
        for (i in 1..env.nProcesses) {
            if (i != env.processId) {
                env.send(i, AppendEntryRpc(curTerm(), storage.readLog(nextIndex[i] - 1)?.id ?: LogId(0, 0), commitIndex, null))
            }
        }
        env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
    }

    private fun becomeLeader() {
        state = State.LEADER
        for (i in 1..env.nProcesses) {
            nextIndex[i] = storage.readLastLogId().index + 1
            matchIndex[i] = 0
        }

        while (commands.isNotEmpty()) {
            onClientCommand(commands.poll())
        }

        sendHeartbeats()
    }

    private fun becomeFollower() {
        state = State.FOLLOWER
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun updateStateMachine() {
        while (commitIndex > lastApplied) {
            lastApplied++
            val entry = storage.readLog(lastApplied)
            val command = entry!!.command
            val result = machine.apply(command)
            if (command.processId == env.processId && entry.id.term == curTerm() && state == State.LEADER) {
                env.onClientCommandResult(result)
            } else if (state == State.LEADER && entry.id.term == curTerm()) {
                env.send(command.processId, ClientCommandResult(curTerm(), result))
            }
        }
    }

    private fun updateCommit() {
        var newCommit = matchIndex.sorted()[matchIndex.size - env.nProcesses / 2]
        while (newCommit > commitIndex) {
            val entry = storage.readLog(newCommit)
            if (entry != null && entry.id.term == curTerm()) {
                break
            }
            newCommit--
        }
        commitIndex = newCommit

        updateStateMachine()
    }

    override fun onTimeout() {
        if (state == State.LEADER) {
            sendHeartbeats()
        } else {
            startElection()
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        var f = false
        if (message.term > curTerm()) {
            storage.writePersistentState(PersistentState(message.term, null))
            leaderId = null
            f = true
            if (state != State.FOLLOWER) {
                becomeFollower()
            }
        }

        if (message is AppendEntryRpc) {
            if (state != State.FOLLOWER && curTerm() == message.term) {
                leaderId = null
                becomeFollower()
            }

            if (message.term < curTerm()) {
                env.send(srcId, AppendEntryResult(curTerm(), null))
                return
            }

            if (state == State.FOLLOWER) {
                env.startTimeout(Timeout.ELECTION_TIMEOUT)

                leaderId = srcId

                sendCommands(leaderId!!)

                if (message.prevLogId.index != 0) {
                    val entry = storage.readLog(message.prevLogId.index)
                    if (entry == null || entry.id.term != message.prevLogId.term) {
                        env.send(srcId, AppendEntryResult(curTerm(), null))
                        return
                    }
                }

                if (message.entry == null) {
                    env.send(srcId, AppendEntryResult(curTerm(), message.prevLogId.index))

                    if (message.leaderCommit > commitIndex) {
                        commitIndex = message.leaderCommit.coerceAtMost(storage.readLastLogId().index)
                    }

                    updateStateMachine()

                    return
                }

                storage.appendLogEntry(message.entry)

                if (message.leaderCommit > commitIndex) {
                    commitIndex = message.leaderCommit.coerceAtMost(storage.readLastLogId().index)
                }

                updateStateMachine()

                env.send(srcId, AppendEntryResult(curTerm(), message.entry.id.index))
            }
        } else if (message is AppendEntryResult) {
            nuSubsequentTakSubsequent = true
            if (state == State.LEADER) {
                if (message.term < curTerm()) {
                    return
                }
                if (message.lastIndex != null) {
                    matchIndex[srcId] = matchIndex[srcId].coerceAtLeast(message.lastIndex)
                    nextIndex[srcId] = matchIndex[srcId] + 1
                    updateCommit()
                } else {
                    if (matchIndex[srcId] + 1 == nextIndex[srcId]) {
                        matchIndex[srcId] = 0
                        nextIndex[srcId] = storage.readLastLogId().index + 1
                    } else {
                        nextIndex[srcId] = (nextIndex[srcId] - 1).coerceAtLeast(matchIndex[srcId])
                    }
                }
                if (storage.readLastLogId().index >= nextIndex[srcId]) {
                    env.send(
                        srcId, AppendEntryRpc(
                            curTerm(), storage.readLog(nextIndex[srcId] - 1)?.id ?: LogId(0, 0),
                            commitIndex, storage.readLog(nextIndex[srcId])
                        )
                    )
                }
            }
        } else if (message is RequestVoteRpc) {
            if ((message.term < curTerm())
                || ((votedFor() != null) && (votedFor()!! != srcId))
                || ((votedFor() == null) && (message.lastLogId < storage.readLastLogId()))
            ) {
                if ((votedFor() == null) && (message.lastLogId < storage.readLastLogId())) {
                    env.startTimeout(Timeout.ELECTION_TIMEOUT)
                }
                env.send(srcId, RequestVoteResult(curTerm(), false))
            } else {
                if (votedFor() == null && state == State.FOLLOWER) {
                    env.startTimeout(Timeout.ELECTION_TIMEOUT)
                }
                storage.writePersistentState(PersistentState(curTerm(), srcId))
                env.send(srcId, RequestVoteResult(curTerm(), true))
            }
        } else if (message is RequestVoteResult) {
            if (state == State.CANDIDATE && message.term == curTerm() && message.voteGranted) {
                votesCnt++
                if (votesCnt * 2 > env.nProcesses) {
                    becomeLeader()
                }
            }
        } else if (message is ClientCommandRpc) {
            onClientCommand(message.command)
        } else if (message is ClientCommandResult) {
            if (message.term == curTerm()) {
                leaderId = srcId
                sendCommands(leaderId!!)
                if (f) {
                    env.startTimeout(Timeout.ELECTION_TIMEOUT)
                }
            }
            env.onClientCommandResult(message.result)
        }
    }

    private fun broadcast() {
        for (i in 1..env.nProcesses) {
            if (i != env.processId && (storage.readLastLogId().index >= nextIndex[i])) {
                env.send(
                    i, AppendEntryRpc(
                        curTerm(), storage.readLog(nextIndex[i] - 1)?.id ?: LogId(0, 0),
                        commitIndex, storage.readLog(nextIndex[i])
                    )
                )
            }
        }
    }

    private fun sendCommands(id: Int) {
        while (commands.isNotEmpty()) {
            val command = commands.poll()
            env.send(id, ClientCommandRpc(curTerm(), command))
        }
    }

    override fun onClientCommand(command: Command) {
        if (state == State.LEADER) {
            storage.appendLogEntry(LogEntry(LogId(storage.readLastLogId().index + 1, curTerm()), command))
            if (nuSubsequentTakSubsequent) {
                broadcast()
                nuSubsequentTakSubsequent = false
            }
        } else {
            commands.add(command)
            if (leaderId != null) {
                sendCommands(leaderId!!)
            }
        }
    }

    private enum class State {
        FOLLOWER, CANDIDATE, LEADER
    }
}
